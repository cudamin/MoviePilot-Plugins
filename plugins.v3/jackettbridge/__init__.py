# -*- coding: utf-8 -*-
import copy
import re
import sys
import threading
import time
import traceback
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, quote_plus, urlparse

import requests
from apscheduler.triggers.cron import CronTrigger
from fastapi.concurrency import run_in_threadpool

from app.application.configuration import get_configured_system_config
from app.db.oper.site import SiteOper
from app.sdk.config import settings
from app.sdk.logging import logger
from app.sdk.media import TorrentInfo
from app.sdk.network import RequestUtils, SitesHelper
from app.plugins import _PluginBase
from app.schemas.types import EventType, MediaSource, MediaType, SystemConfigKey

# Torznab 命名空间
TORZNAB_NS = "http://torznab.com/schemas/2015/feed"
# 默认同步周期：每天零点
DEFAULT_CRON = "0 0 * * *"
# 默认检索超时（秒）
DEFAULT_SEARCH_TIMEOUT = 30
# 默认单个索引器返回条数
DEFAULT_RESULT_NUM = 100
# 插件资源源并发检索的索引器数量上限
DEFAULT_PARALLEL_INDEXERS = 4

# 权威搜索帧的方法名白名单。
#
# 宿主 V3 把搜索 provider 拆成 app/chain/search/provider.py 里的 owner，再由
# app/chain/search/facade.py 以「类属性赋值」的方式挂到 SearchChain 上
# （_SearchChain__search_all_sites = SearchProviderOwner._search_all_sites 等）。
# 别名赋值不产生新函数对象，函数身份仍是 provider.py 里定义的那一个，因此
# co_name 保持原样、不受 Facade 的名字改写影响，可以稳定用于帧匹配：
#   · 同步链：_search_all_sites(keyword, mediainfo, sites, page, area, mtype)
#   · 异步链：_iter_torrent_events(keyword, mediainfo, sites, page, area, mtype)
# 两个方法都把调用方选中的站点 ID 收在形参 sites 上，但向下调用插件资源源时
# （search_plugin_torrents / async_search_plugin_torrents）刻意不传该值，所以只能
# 从调用栈的帧局部变量里取回。
SITES_FRAME_NAMES = frozenset({
    "_search_all_sites",
    "_iter_torrent_events",
    "_async_search_all_sites",
    "_async_search_all_sites_stream",
    "_iter_subtitle_events",
    "_async_search_subtitles_all_sites",
    "_async_search_subtitles_all_sites_stream",
})
# 帧回溯深度上限：正常链路只需 6 层，留出余量同时避免异常栈上长时间遍历
FRAME_WALK_LIMIT = 32

# 站点选择来源标记，写入 /status 供排查
SITES_SOURCE_EXPLICIT = "explicit"
SITES_SOURCE_FRAME = "frame"
SITES_SOURCE_SYSTEM = "system"
SITES_SOURCE_UNLIMITED = "unlimited"


class JackettBridge(_PluginBase):
    """
    Jackett 索引器桥接插件。

    参考 nas-tools 的 Jackett 实现：登录 Jackett 拉取已配置的索引器，注册为 MoviePilot
    虚拟站点，并通过 Torznab API 完成资源检索。
    """

    # 插件名称
    plugin_name = "jackettBridge"
    # 插件描述
    plugin_desc = "将 Jackett 中已配置的索引器接入 MoviePilot 搜索与订阅。"
    # 插件图标
    plugin_icon = "Jackett_A.png"
    # 插件版本
    plugin_version = "1.3.9"
    # 插件标签
    plugin_label = "站点"
    # 插件作者
    plugin_author = "tafei"
    # 作者主页
    author_url = "https://github.com/cudamin"
    # 插件配置项ID前缀
    plugin_config_prefix = "jackettbridge_"
    # 加载顺序
    plugin_order = 16
    # 可使用的用户级别
    auth_level = 1

    # 虚拟站点域名前缀与后缀
    domain_prefix = "jackett-"
    domain_suffix = "extend"

    # 运行时状态默认值
    _enabled = False
    _onlyonce = False
    _host = ""
    _api_key = ""
    _password = ""
    _cron = DEFAULT_CRON
    _search_timeout = DEFAULT_SEARCH_TIMEOUT
    _result_num = DEFAULT_RESULT_NUM
    _indexers_authoritative = False
    # 以下列表在 init_plugin 中整体重新赋值，不做原地修改
    _selected_indexers: List[str] = []
    _indexer_catalog: List[Dict[str, str]] = []
    _indexers: List[Dict[str, Any]] = []
    _sync_lock = threading.Lock()
    # 站点选择诊断：最近一次插件资源源检索实际采用的选中站点与来源
    _last_selected_sites: Optional[List[int]] = None
    _last_sites_source = SITES_SOURCE_UNLIMITED
    _last_frame_sites: Optional[List[int]] = None

    def init_plugin(self, config: dict = None) -> None:
        """
        根据插件配置初始化运行状态，并在需要时后台同步索引器。
        """
        self.stop_service()
        self.sites_helper = SitesHelper()
        self.site_oper = SiteOper()
        self._enabled = False
        self._onlyonce = False
        self._host = ""
        self._api_key = ""
        self._password = ""
        self._cron = DEFAULT_CRON
        self._search_timeout = DEFAULT_SEARCH_TIMEOUT
        self._result_num = DEFAULT_RESULT_NUM
        self._selected_indexers: List[str] = []
        self._indexer_catalog: List[Dict[str, str]] = []
        self._indexers: List[Dict[str, Any]] = []
        self._indexers_authoritative = False
        self._last_selected_sites = None
        self._last_sites_source = SITES_SOURCE_UNLIMITED
        self._last_frame_sites = None

        # 恢复上次同步的索引器快照，避免重启后检索失效
        saved = self.get_data("indexers") or []
        if isinstance(saved, list):
            self._indexers = [item for item in saved if isinstance(item, dict)]

        if not config:
            return

        saved_config = self.get_config() or {}
        self._enabled = bool(config.get("enabled"))
        self._onlyonce = bool(config.get("onlyonce"))
        self._host = self.__normalize_host(config.get("host"))
        self._api_key = str(config.get("api_key") or "").strip()
        self._password = str(config.get("password") or "")
        self._cron = str(config.get("cron") or "").strip() or DEFAULT_CRON
        self._search_timeout = self.__to_int(config.get("search_timeout"), DEFAULT_SEARCH_TIMEOUT, 5, 600)
        self._result_num = self.__to_int(config.get("result_num"), DEFAULT_RESULT_NUM, 10, 500)
        # 表单保存时可能不带多选快照，回退到已保存配置
        self._selected_indexers = self.__normalize_str_list(
            config.get("selected_indexers") if "selected_indexers" in config
            else saved_config.get("selected_indexers")
        )
        self._indexer_catalog = self.__normalize_catalog(
            config.get("indexer_catalog")
        ) or self.__normalize_catalog(saved_config.get("indexer_catalog"))

        if not self._enabled:
            # 插件关闭：清理站点管理中的虚拟站点，保留索引器快照以便重新开启后自动恢复
            self.__cleanup_managed_sites()
            return

        if self._onlyonce:
            self._onlyonce = False
            self.__update_config()
            logger.info(f"【{self.plugin_name}】立即同步索引器")
            self.__start_sync_thread()
            return

        if self._enabled and self._host and self._api_key and not self._indexers:
            logger.info(f"【{self.plugin_name}】后台异步同步索引器，避免阻塞插件加载")
            self.__start_sync_thread()
        elif self._enabled and self._indexers:
            # 使用本地快照先把虚拟站点注册回来；插件版本变化或快照缺媒体分类声明时
            # 改走完整同步，确保站点索引助手中的注册结构更新到当前版本
            last_version = self.get_data("synced_plugin_version")
            if last_version != self.plugin_version or any(
                    not item.get("category") for item in self._indexers):
                logger.info(
                    f"【{self.plugin_name}】插件版本 {last_version or '未知'} → "
                    f"{self.plugin_version}，执行完整同步更新索引器注册结构"
                )
                self.__start_sync_thread()
            else:
                self.__start_sync_thread(restore_only=True)

    def get_state(self) -> bool:
        """
        获取插件启用状态。
        """
        return bool(self._enabled and self._host and self._api_key)

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        返回插件远程命令列表。
        """
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册索引器定时同步服务。
        """
        if not self.get_state() or not self._cron:
            return []
        try:
            trigger = CronTrigger.from_crontab(self._cron)
        except Exception as e:
            logger.warn(f"【{self.plugin_name}】同步周期 {self._cron} 格式错误，回退为 {DEFAULT_CRON}：{str(e)}")
            trigger = CronTrigger.from_crontab(DEFAULT_CRON)
        return [{
            "id": "JackettBridgeSyncIndexers",
            "name": "Jackett 索引器同步",
            "trigger": trigger,
            "func": self.sync_indexers,
            "kwargs": {}
        }]

    def get_module(self) -> Dict[str, Any]:
        """
        声明劫持的系统模块方法，接入站点检索链路。
        """
        return {
            "search_torrents": self.search_torrents,
            "async_search_torrents": self.async_search_torrents,
            "refresh_torrents": self.refresh_torrents,
            "async_refresh_torrents": self.async_refresh_torrents,
        }

    def get_api(self) -> List[Dict[str, Any]]:
        """
        返回插件 API 列表。
        """
        return [
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "summary": "获取 Jackett 桥接状态"
            },
            {
                "path": "/test",
                "endpoint": self.api_test,
                "methods": ["GET"],
                "summary": "测试 Jackett 连通性"
            },
            {
                "path": "/sync",
                "endpoint": self.api_sync,
                "methods": ["GET"],
                "summary": "立即同步 Jackett 索引器"
            }
        ]

    def stop_service(self) -> None:
        """
        停止插件后台服务并释放资源。

        卸载流程会先从已安装列表移除本插件再执行停止，据此区分卸载与重载/关停：
        仅在卸载场景清理站点管理中由本插件托管的虚拟站点。
        """
        if self.is_clone:
            return
        try:
            installed = self.systemconfig.get(SystemConfigKey.UserInstalledPlugins) or []
        except Exception as e:
            logger.warn(f"【{self.plugin_name}】读取已安装插件列表失败，跳过卸载清理：{str(e)}")
            return
        if self.__class__.__name__ in installed:
            return
        logger.info(f"【{self.plugin_name}】插件已卸载，清理站点管理中的虚拟站点")
        self.__cleanup_managed_sites()

    # ------------------------------------------------------------------ 配置

    @staticmethod
    def __normalize_host(value: Any) -> str:
        """
        规范化 Jackett 地址，补全协议并去除末尾斜杠。
        """
        host = str(value or "").strip()
        if not host:
            return ""
        if not host.startswith("http"):
            host = f"http://{host}"
        return host.rstrip("/")

    @staticmethod
    def __to_int(value: Any, default: int, minimum: int, maximum: int) -> int:
        """
        将配置值转换为限定范围内的整数。
        """
        try:
            number = int(str(value).strip())
        except (TypeError, ValueError):
            return default
        return max(minimum, min(maximum, number))

    @staticmethod
    def __normalize_str_list(value: Any) -> List[str]:
        """
        将多选配置规范化为字符串列表。
        """
        if not value:
            return []
        if isinstance(value, str):
            items = re.split(r"[,\n]", value)
        elif isinstance(value, (list, tuple, set)):
            items = list(value)
        else:
            return []
        result = []
        for item in items:
            if isinstance(item, dict):
                item = item.get("value")
            text = str(item or "").strip()
            if text and text not in result:
                result.append(text)
        return result

    @staticmethod
    def __normalize_catalog(value: Any) -> List[Dict[str, str]]:
        """
        规范化索引器目录快照，用于多选组件展示。
        """
        if not isinstance(value, list):
            return []
        catalog = []
        for item in value:
            if not isinstance(item, dict):
                continue
            indexer_id = str(item.get("value") or item.get("id") or "").strip()
            title = str(item.get("title") or item.get("name") or indexer_id).strip()
            if indexer_id:
                catalog.append({"title": title, "value": indexer_id})
        return catalog

    def __update_config(self) -> None:
        """
        持久化当前插件配置，合并到已保存配置之上，避免覆盖并发保存的其他键。
        """
        config = self.get_config() or {}
        config.update({
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "host": self._host,
            "api_key": self._api_key,
            "password": self._password,
            "cron": self._cron,
            "search_timeout": self._search_timeout,
            "result_num": self._result_num,
            "selected_indexers": self._selected_indexers,
            "indexer_catalog": self._indexer_catalog,
        })
        self.update_config(config)

    # -------------------------------------------------------------- 站点选择

    @staticmethod
    def __normalize_site_ids(value: Any) -> List[int]:
        """
        将任意形态的站点选择规范化为去重的整型 ID 列表。
        """
        if value is None or value == "":
            return []
        if isinstance(value, (str, bytes, int, float)):
            candidates: List[Any] = [value]
        elif isinstance(value, (list, tuple, set, frozenset)):
            candidates = list(value)
        else:
            return []
        result: List[int] = []
        for item in candidates:
            if isinstance(item, bool):
                continue
            try:
                number = int(str(item).strip()) if isinstance(item, str) else int(item)
            except (TypeError, ValueError):
                continue
            if number not in result:
                result.append(number)
        return result

    def __extract_selected_sites(self) -> Optional[List[int]]:
        """
        从权威搜索帧中取回宿主本次搜索选中的站点 ID。

        宿主调用插件资源源时不传 sites，但调用栈上仍保留权威帧：
          · 同步链 search_torrents ← _execute_plugin_provider_sequence
            ← execute_plugin_modules ← search_plugin_torrents ← _search_all_sites
          · 异步链 async_search_torrents ← _async_call ← async_execute_plugin_modules
            ← async_search_plugin_torrents ← _iter_torrent_events
        两链都在同一线程内直连，帧链完整（异步链的 _async_call 对协程函数直接
        await、不落线程池），因此可以从帧局部变量里读到 sites。

        :return: 选中站点 ID 列表；未命中权威帧或该帧未做选择时返回 None
        """
        frame = sys._getframe(1)
        depth = 0
        try:
            while frame is not None and depth < FRAME_WALK_LIMIT:
                if frame.f_code.co_name in SITES_FRAME_NAMES:
                    return self.__normalize_site_ids(frame.f_locals.get("sites"))
                frame = frame.f_back
                depth += 1
        except Exception as e:  # 帧回溯属于尽力而为，任何异常都退化为「未命中」
            logger.warn(f"【{self.plugin_name}】回溯权威搜索帧失败：{str(e)}")
        finally:
            del frame
        return None

    def __system_indexer_sites(self) -> List[int]:
        """
        读取系统「搜索站点」（IndexerSites）配置，与宿主 _selected_site_ids 同源。
        """
        try:
            configured = get_configured_system_config().get(SystemConfigKey.IndexerSites)
        except Exception as e:
            logger.warn(f"【{self.plugin_name}】读取系统「搜索站点」配置失败：{str(e)}")
            return []
        return self.__normalize_site_ids(configured)

    def __resolve_effective_sites(self, sites: Optional[List[int]] = None) -> Optional[List[int]]:
        """
        解析本次插件资源源检索应采用的有效站点范围。

        回退顺序与宿主 _selected_site_ids 保持一致：
          显式传入 → 权威帧提取 → 系统「搜索站点」→ 空（不限站点）
        """
        explicit = self.__normalize_site_ids(sites)
        if explicit:
            self.__record_sites_choice(explicit, SITES_SOURCE_EXPLICIT)
            return explicit

        extracted = self.__extract_selected_sites()
        self._last_frame_sites = extracted
        if extracted:
            self.__record_sites_choice(extracted, SITES_SOURCE_FRAME)
            return extracted

        configured = self.__system_indexer_sites()
        if configured:
            self.__record_sites_choice(configured, SITES_SOURCE_SYSTEM)
            return configured

        self.__record_sites_choice(None, SITES_SOURCE_UNLIMITED)
        return None

    def __record_sites_choice(self, sites: Optional[List[int]], source: str) -> None:
        """
        记录本次站点选择结果，供 /status 诊断。
        """
        self._last_selected_sites = list(sites) if sites else None
        self._last_sites_source = source

    # ------------------------------------------------------------------ 同步

    def __start_sync_thread(self, restore_only: bool = False) -> None:
        """
        在后台线程中同步索引器，避免阻塞插件加载。
        """
        threading.Thread(
            target=self.sync_indexers,
            kwargs={"restore_only": restore_only},
            daemon=True,
            name="JackettBridgeSyncIndexers",
        ).start()

    def sync_indexers(self, restore_only: bool = False) -> None:
        """
        同步 Jackett 索引器到 MoviePilot 站点体系。

        :param restore_only: 仅使用本地快照恢复虚拟站点，不请求 Jackett
        """
        if not self._sync_lock.acquire(blocking=False):
            logger.info(f"【{self.plugin_name}】已有同步任务在执行，跳过本次同步")
            return
        try:
            if not restore_only:
                if not self._host or not self._api_key:
                    logger.warn(f"【{self.plugin_name}】未配置 Jackett 地址或 API Key，无法同步")
                    return
                indexers = self.get_indexers()
                if indexers is None:
                    logger.warn(f"【{self.plugin_name}】获取索引器失败，保留上次同步结果")
                    return
                self._indexers = indexers
                self.save_data("indexers", indexers)
                self.save_data("last_sync", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                self.save_data("synced_plugin_version", self.plugin_version)
                self.__update_config()

            if not self._indexers:
                logger.info(f"【{self.plugin_name}】没有可用索引器")

            registered, updated = self.__sync_helper_indexers()
            self.__sync_site_records()
            logger.info(
                f"【{self.plugin_name}】同步完成：索引器 {len(self._indexers)} 个，"
                f"新注册 {registered} 个、更新 {updated} 个"
            )
        except Exception as e:
            logger.error(f"【{self.plugin_name}】同步索引器出错：{str(e)}\n{traceback.format_exc()}")
        finally:
            self._sync_lock.release()

    def __headers(self) -> Dict[str, str]:
        """
        构造 Jackett 请求头。
        """
        return {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "User-Agent": settings.USER_AGENT,
            "X-Api-Key": self._api_key,
            "Accept": "application/json, text/javascript, */*; q=0.01",
        }

    def __login_cookies(self) -> Optional[dict]:
        """
        使用管理密码登录 Jackett 面板换取 Cookie，未配置密码时返回 None。
        """
        if not self._password:
            return None
        try:
            session = requests.session()
            res = RequestUtils(headers=self.__headers(), session=session).post_res(
                url=f"{self._host}/UI/Dashboard",
                data={"password": self._password},
            )
            if res and session.cookies:
                return session.cookies.get_dict()
            logger.warn(f"【{self.plugin_name}】Jackett 面板登录失败，未获取到 Cookie")
        except Exception as e:
            logger.warn(f"【{self.plugin_name}】Jackett 面板登录异常：{str(e)}")
        return None

    def __fetch_indexers_torznab(self) -> Optional[List[Dict[str, Any]]]:
        """
        通过 Torznab `t=indexers` 接口获取已配置索引器列表。

        该接口仅需 apikey，即使 Jackett 设置了管理密码也可访问，返回 XML。

        :return: 索引器原始信息列表，失败时返回 None
        """
        try:
            res = RequestUtils(
                headers={"User-Agent": settings.USER_AGENT},
                timeout=15,
            ).get_res(
                f"{self._host}/api/v2.0/indexers/all/results/torznab/api",
                params={"apikey": self._api_key, "t": "indexers", "configured": "true"},
            )
        except Exception as e:
            logger.warn(f"【{self.plugin_name}】请求 Torznab 索引器列表异常：{str(e)}")
            return None
        if not res:
            logger.warn(f"【{self.plugin_name}】Torznab 索引器列表请求无响应，请检查地址与网络")
            return None
        if res.status_code >= 400:
            logger.warn(
                f"【{self.plugin_name}】Torznab 索引器列表请求失败：HTTP {res.status_code}，"
                f"请检查 API Key 是否正确"
            )
            return None
        content = (res.text or "").strip()
        if not content.startswith("<"):
            logger.warn(f"【{self.plugin_name}】Torznab 索引器列表返回非 XML 内容，尝试管理接口")
            return None
        try:
            root = ET.fromstring(content)
        except Exception as e:
            logger.warn(f"【{self.plugin_name}】Torznab 索引器列表解析失败：{str(e)}")
            return None
        if root.tag == "error":
            logger.error(
                f"【{self.plugin_name}】Jackett 返回错误："
                f"{root.attrib.get('description') or root.attrib.get('code')}"
            )
            return None
        items: List[Dict[str, Any]] = []
        for node in root.findall("indexer"):
            indexer_id = str(node.attrib.get("id") or "").strip()
            if not indexer_id:
                continue
            configured = str(node.attrib.get("configured") or "").strip().lower()
            items.append({
                "id": indexer_id,
                "name": (node.findtext("title") or "").strip() or indexer_id,
                "type": (node.findtext("type") or "").strip(),
                "configured": False if configured == "false" else True,
            })
        if not items:
            logger.warn(f"【{self.plugin_name}】Jackett 未返回任何索引器，请先在 Jackett 中添加索引器")
            return None
        return items

    def __fetch_indexers_admin_api(self) -> Optional[List[Dict[str, Any]]]:
        """
        通过 Jackett 管理 API 获取索引器列表，设置了管理密码时需要 Cookie。

        :return: 索引器原始信息列表，失败时返回 None
        """
        cookies = self.__login_cookies()
        try:
            res = RequestUtils(
                headers=self.__headers(),
                cookies=cookies,
                timeout=15,
            ).get_res(
                f"{self._host}/api/v2.0/indexers?configured=true",
                params={"apikey": self._api_key},
            )
        except Exception as e:
            logger.error(f"【{self.plugin_name}】请求索引器列表异常：{str(e)}")
            return None

        if not res:
            logger.warn(f"【{self.plugin_name}】索引器列表请求无响应，请检查地址与网络")
            return None
        if res.status_code >= 400:
            logger.error(
                f"【{self.plugin_name}】索引器列表请求失败：HTTP {res.status_code}，"
                f"若设置了管理密码请在插件中填写"
            )
            return None
        content = (res.text or "").strip()
        if not content.startswith("[") and not content.startswith("{"):
            logger.error(
                f"【{self.plugin_name}】索引器列表返回非 JSON 内容"
                f"（HTTP {res.status_code}），请检查地址、API Key 与管理密码"
            )
            return None
        try:
            data = res.json()
        except Exception as e:
            logger.error(f"【{self.plugin_name}】索引器列表返回数据解析失败：{str(e)}")
            return None
        if not isinstance(data, list):
            logger.error(f"【{self.plugin_name}】索引器列表返回数据格式异常")
            return None
        return [item for item in data if isinstance(item, dict)]

    def get_indexers(self) -> Optional[List[Dict[str, Any]]]:
        """
        获取 Jackett 中已配置的索引器并转换为虚拟站点结构。

        :return: 索引器列表，请求失败时返回 None
        """
        indexers = self.__fetch_indexers()
        if indexers is None:
            return None
        self._indexers_authoritative = True
        self._indexer_catalog = [
            {"title": item.get("origin_name") or item.get("name"), "value": item.get("indexer_id")}
            for item in indexers
        ]
        return self.__apply_selection(indexers)

    def __fetch_indexers(self) -> Optional[List[Dict[str, Any]]]:
        """
        请求 Jackett 并构造索引器列表，不修改任何插件运行状态。

        :return: 全部已配置索引器列表，请求失败时返回 None
        """
        # 优先使用 Torznab t=indexers 接口：只需 apikey，兼容设置了管理密码的 Jackett
        data = self.__fetch_indexers_torznab()
        if data is None:
            # 回退管理 API（需要管理密码换取 Cookie，可获取更完整信息）
            data = self.__fetch_indexers_admin_api()
        if data is None:
            return None
        indexers = []
        skipped = 0
        for item in data:
            if not isinstance(item, dict):
                continue
            indexer_id = str(item.get("id") or "").strip()
            indexer_name = str(item.get("name") or "").strip() or indexer_id
            if not indexer_id:
                continue
            if item.get("configured") is False:
                skipped += 1
                continue
            indexers.append(self.__build_indexer(indexer_id, indexer_name, item.get("type")))
        logger.info(
            f"【{self.plugin_name}】Jackett 返回 {len(indexers)} 个已配置索引器，跳过未配置 {skipped} 个"
        )
        return indexers

    def __build_indexer(self, indexer_id: str, indexer_name: str,
                        indexer_type: Optional[str] = None) -> Dict[str, Any]:
        """
        构造 MoviePilot 虚拟索引器结构。

        :param indexer_id: Jackett 索引器 ID
        :param indexer_name: Jackett 索引器名称
        :param indexer_type: Jackett 索引器类型（public/private/semi-private）
        """
        privacy = str(indexer_type or "").strip().lower() or "unknown"
        domain = self.__build_domain(indexer_id)
        # parser/plugin 标记用于识别本插件托管的虚拟站点。MoviePilot V3 的按站点搜索只走宿主
        # 蜘蛛、不再回调插件，而蜘蛛只要 search 字段为真值就会去抓虚拟域名（jackett-*.extend）；
        # 因此这里保持 search 为空字典，让宿主模块直接跳过虚拟站点，资源统一由「插件资源源」
        # 通道（search_torrents / refresh_torrents 收到空 site）返回。
        return {
            "id": f"{self.plugin_name}-{indexer_id}",
            "name": f"{self.plugin_name}-{indexer_name}",
            "origin_name": indexer_name,
            "indexer_id": indexer_id,
            "url": f"{self._host}/api/v2.0/indexers/{indexer_id}/results/torznab/",
            "rss": self.__build_rss_url(indexer_id),
            "domain": domain,
            "public": privacy == "public",
            "privacy": privacy,
            "proxy": False,
            "result_num": self._result_num,
            "timeout": 5,
            "parser": self.plugin_name,
            "plugin": self.plugin_name,
            # 声明支持的媒体分类：音乐搜索的站点列表（/site/media/music）要求
            # category.music 非空才收录本插件站点；电影/剧集声明后同样显式匹配
            "category": {"movie": [2000], "tv": [5000], "music": [3000]},
            "search": {},
            "browse": {"path": ""},
            "torrents": {"list": {"selector": ""}, "fields": {}},
        }

    def __build_rss_url(self, indexer_id: str) -> str:
        """
        生成索引器的 Torznab 最新种子 RSS 地址，供订阅刷新（rss 模式）直接解析。

        :param indexer_id: Jackett 索引器 ID
        """
        params = [
            ("apikey", self._api_key),
            ("t", "search"),
            ("q", ""),
            ("cat", ",".join(str(item) for item in self.get_cat(None))),
            ("limit", self._result_num),
        ]
        return f"{self._host}/api/v2.0/indexers/{indexer_id}/results/torznab/api?" \
               f"{urlencode(params, quote_via=quote_plus)}"

    def __build_domain(self, indexer_id: str) -> str:
        """
        生成虚拟站点域名，Jackett 索引器 ID 中的非法字符会被替换。
        """
        slug = re.sub(r"[^a-z0-9-]+", "-", str(indexer_id).lower()).strip("-") or "unknown"
        return f"{self.domain_prefix}{slug}.{self.domain_suffix}"

    def __is_managed_domain(self, domain: str) -> bool:
        """
        判断域名是否由本插件托管。
        """
        if not domain:
            return False
        raw = domain
        if "://" in raw:
            raw = urlparse(raw).hostname or raw
        raw = raw.strip("/").lower()
        return raw.startswith(self.domain_prefix) and raw.endswith(f".{self.domain_suffix}")

    def __is_managed_site(self, site: dict) -> bool:
        """
        判断站点是否属于本插件托管的虚拟索引器。
        """
        if not site:
            return False
        if site.get("plugin") == self.plugin_name or site.get("parser") == self.plugin_name:
            return True
        if self.__is_managed_domain(site.get("domain") or ""):
            return True
        return str(site.get("name") or "").startswith(f"{self.plugin_name}-")

    def __apply_selection(self, indexers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        按多选配置过滤索引器，未选择时桥接全部索引器。
        """
        if not self._selected_indexers:
            return indexers
        filtered = [item for item in indexers if item.get("indexer_id") in self._selected_indexers]
        if len(filtered) != len(indexers):
            logger.info(
                f"【{self.plugin_name}】按多选过滤索引器：{len(indexers)} → {len(filtered)}"
            )
        return filtered

    def __sync_helper_indexers(self) -> Tuple[int, int]:
        """
        将虚拟索引器注册或更新到站点索引助手。

        宿主 SitesHelper 未提供移除接口，索引器被移除或多选收窄后，已注册条目会
        残留至进程重启；因 search 为空字典，宿主蜘蛛会跳过这些条目，不影响检索。

        :return: (新注册数量, 更新数量)
        """
        registered = 0
        updated = 0
        for indexer in self._indexers:
            domain = indexer.get("domain")
            if not domain:
                continue
            exists = self.sites_helper.get_indexer(domain)
            if not exists:
                self.sites_helper.add_indexer(domain, copy.deepcopy(indexer))
                registered += 1
            elif not self.__indexer_matches(exists, indexer):
                self.sites_helper.add_indexer(domain, copy.deepcopy(indexer))
                updated += 1
        return registered, updated

    @staticmethod
    def __indexer_matches(exists: Any, indexer: Dict[str, Any]) -> bool:
        """
        判断已注册的索引器是否与目标结构一致。
        """
        if not isinstance(exists, dict):
            return False
        for key in ("id", "name", "url", "rss", "domain", "public", "privacy", "proxy",
                    "parser", "plugin", "result_num", "timeout", "category"):
            if exists.get(key) != indexer.get(key):
                return False
        # search 只比较「是否可搜索」的真值形态：已注册条目可能被宿主补充额外键，
        # 逐键比较会导致每次同步都重新注册。
        if bool(exists.get("search")) != bool(indexer.get("search")):
            return False
        return True

    def __get_managed_site_records(self) -> List[Any]:
        """
        读取数据库中由本插件托管的站点记录。
        """
        try:
            sites = self.site_oper.list_order_by_pri() or []
        except Exception as e:
            logger.warn(f"【{self.plugin_name}】读取站点列表失败，跳过旧站点清理：{str(e)}")
            return []
        return [site for site in sites if self.__is_managed_domain(getattr(site, "domain", ""))]

    def __cleanup_managed_sites(self) -> int:
        """
        清理站点管理中由本插件托管的全部虚拟站点。

        插件关闭时调用：仅删除站点表记录，保留已同步的索引器快照（get_data("indexers")），
        以便重新开启后通过 restore_only 同步自动恢复。

        :return: 被删除的站点数量
        """
        removed = 0
        for site in self.__get_managed_site_records():
            site_id = getattr(site, "id", None)
            if not site_id:
                continue
            try:
                self.site_oper.delete(site_id)
                removed += 1
            except Exception as e:
                logger.warn(
                    f"【{self.plugin_name}】删除虚拟站点失败（id={site_id}）：{str(e)}"
                )
        if removed:
            self.eventmanager.send_event(EventType.SiteDeleted, {"plugin_id": self.plugin_name})
            logger.info(
                f"【{self.plugin_name}】插件已关闭，清理站点管理中的虚拟站点 {removed} 个"
            )
        return removed

    def __sync_site_records(self) -> Tuple[List[int], List[int]]:
        """
        同步站点表记录：新增、更新并清理已失效的虚拟站点。

        :return: (当前站点ID列表, 被删除的站点ID列表)
        """
        current_domains = {item.get("domain") for item in self._indexers if item.get("domain")}
        site_ids: List[int] = []
        removed_site_ids: List[int] = []
        created = updated = removed = 0

        for indexer in self._indexers:
            domain = indexer.get("domain")
            if not domain:
                continue
            payload = {
                "name": indexer.get("name"),
                "domain": domain,
                "url": f"https://{domain}/",
                "pri": 0,
                "public": 1 if indexer.get("public") else 0,
                "proxy": 1 if indexer.get("proxy") else 0,
                "rss": indexer.get("rss") or "",
                "render": 0,
                "timeout": 5,
                "is_active": True,
                "note": {
                    "managed_by": self.plugin_name,
                    "indexer_id": indexer.get("indexer_id"),
                    "privacy": indexer.get("privacy") or "unknown",
                },
            }
            site = self.site_oper.get_by_domain(domain)
            if not site:
                success, _ = self.site_oper.add(**payload)
                site = self.site_oper.get_by_domain(domain)
                if success:
                    created += 1
            else:
                changes = {k: v for k, v in payload.items() if getattr(site, k, None) != v}
                if changes:
                    self.site_oper.update(site.id, changes)
                    site = self.site_oper.get_by_domain(domain)
                    updated += 1
            if site and site.id:
                site_ids.append(site.id)
                # 快照记录整型站点 ID，供检索结果归属与按选中站点过滤使用
                indexer["site_id"] = site.id

        if self._indexers_authoritative:
            for site in self.__get_managed_site_records():
                site_id = getattr(site, "id", None)
                if site_id and getattr(site, "domain", "") not in current_domains:
                    self.site_oper.delete(site_id)
                    removed_site_ids.append(site_id)
                    removed += 1

        if created or updated or removed:
            self.eventmanager.send_event(EventType.SiteUpdated, {"plugin_id": self.plugin_name})
            logger.info(
                f"【{self.plugin_name}】同步站点记录：新增 {created} 个、更新 {updated} 个、清理 {removed} 个"
            )
        return site_ids, removed_site_ids

    # ------------------------------------------------------------------ 检索

    def __get_indexer_id(self, site: dict) -> str:
        """
        从站点信息中解析出 Jackett 索引器 ID。
        """
        # 优先使用站点附加信息
        note = site.get("note")
        if isinstance(note, dict) and note.get("indexer_id"):
            return str(note.get("indexer_id"))
        if site.get("indexer_id"):
            return str(site.get("indexer_id"))

        domain = str(site.get("domain") or "")
        if "://" in domain:
            domain = urlparse(domain).hostname or domain
        domain = domain.strip("/").lower()
        # 通过域名反查已同步的索引器，兼容 ID 中含非法字符被替换的情况
        for indexer in self._indexers or []:
            if indexer.get("domain") == domain:
                return str(indexer.get("indexer_id") or "")

        site_id = str(site.get("id") or "")
        prefix = f"{self.plugin_name}-"
        if site_id.startswith(prefix):
            return site_id[len(prefix):]

        url = str(site.get("url") or "")
        matched = re.search(r"/indexers/([^/]+)/results", url)
        if matched:
            return matched.group(1)

        if domain.startswith(self.domain_prefix) and domain.endswith(f".{self.domain_suffix}"):
            return domain[len(self.domain_prefix):-len(f".{self.domain_suffix}")]
        return ""

    @staticmethod
    def get_cat(mtype: Optional[MediaType] = None) -> List[int]:
        """
        获取 Torznab 分类：电影 2000、剧集 5000、音乐 3000。
        """
        if mtype == MediaType.MOVIE:
            return [2000]
        if mtype == MediaType.TV:
            return [5000]
        if mtype == MediaType.MUSIC:
            return [3000]
        return [2000, 3000, 5000]

    def search_torrents(self, site: Optional[dict] = None, keyword: str = None,
                        mtype: Optional[MediaType] = None,
                        page: Optional[int] = 0,
                        sites: Optional[List[int]] = None, **kwargs) -> List[TorrentInfo]:
        """
        检索资源。

        MoviePilot V3 只在「插件资源源」通道调用本方法一次，且固定传入空的 site；此时需要
        自行遍历已桥接索引器并合并结果。site 非空时保留按站点检索的旧行为。

        选中站点由 __resolve_effective_sites 统一解析：显式参数 → 权威调用帧
        （_search_all_sites / _iter_torrent_events）→ 系统「搜索站点」→ 不限。

        :param site: 站点信息，插件资源源调用时为空
        :param keyword: 搜索关键词
        :param mtype: 媒体类型
        :param page: 页码
        :param sites: 选中的站点 ID 列表，空表示按回退顺序解析
        :return: 资源列表
        """
        if not self.get_state():
            return []

        if site:
            return self.__search_managed_site(site=site, keyword=keyword, mtype=mtype, page=page)

        selected = self.__resolve_effective_sites(sites)
        return self.__search_bridged_indexers(keyword=keyword, mtype=mtype, page=page, sites=selected)

    def __search_managed_site(self, site: dict, keyword: str = None,
                              mtype: Optional[MediaType] = None,
                              page: Optional[int] = 0) -> List[TorrentInfo]:
        """
        按指定站点检索单个索引器，站点不属于本插件托管时返回空列表。

        :param site: 站点信息
        :param keyword: 搜索关键词
        :param mtype: 媒体类型
        :param page: 页码
        :return: 资源列表
        """
        if not self.__is_managed_site(site):
            return []

        indexer_id = self.__get_indexer_id(site)
        if not indexer_id:
            logger.warn(
                f"【{self.plugin_name}】无法解析索引器 ID，跳过站点：{site.get('name')}"
                f"（domain={site.get('domain')}）"
            )
            return []

        # 已不在当前桥接列表中的残留站点直接跳过
        if self._indexers:
            current_ids = {str(item.get("indexer_id")) for item in self._indexers}
            if str(indexer_id) not in current_ids:
                logger.warn(
                    f"【{self.plugin_name}】索引器 {indexer_id} 已不在桥接列表，跳过残留站点：{site.get('name')}"
                )
                return []

        return self.__query_jackett(
            indexer_id=indexer_id,
            site=site,
            keyword=keyword,
            mtype=mtype,
            page=page,
        )

    def __search_bridged_indexers(self, keyword: str = None,
                                  mtype: Optional[MediaType] = None,
                                  page: Optional[int] = 0,
                                  sites: Optional[List[int]] = None) -> List[TorrentInfo]:
        """
        插件资源源入口：并发检索已桥接索引器并合并结果。

        :param keyword: 搜索关键词
        :param mtype: 媒体类型
        :param page: 页码
        :param sites: 已解析的选中站点 ID 列表，空表示不限
        :return: 合并后的资源列表
        """
        if not self._indexers:
            logger.warn(f"【{self.plugin_name}】插件资源源检索失败：尚未同步到任何索引器")
            return []

        indexers = [item for item in self._indexers if item.get("indexer_id")]
        if not indexers:
            logger.warn(f"【{self.plugin_name}】插件资源源检索失败：索引器快照缺少索引器 ID")
            return []

        indexers = self.__filter_indexers_by_sites(indexers, sites)
        if not indexers:
            logger.info(
                f"【{self.plugin_name}】选中站点不包含桥接索引器，跳过插件资源源检索"
                f"（来源：{self._last_sites_source}，选中：{sites or '不限'}）"
            )
            return []

        started = time.monotonic()
        results: List[TorrentInfo] = []
        details: List[str] = []
        max_workers = max(1, min(len(indexers), DEFAULT_PARALLEL_INDEXERS))
        logger.info(
            f"【{self.plugin_name}】插件资源源开始检索 {len(indexers)} 个索引器，"
            f"关键词：{keyword}，选中站点：{sites or '不限'}（来源：{self._last_sites_source}）"
        )
        with ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="JackettBridgeSearch",
        ) as pool:
            futures = {
                pool.submit(
                    self.__query_jackett,
                    indexer_id=str(item.get("indexer_id")),
                    site=self.__resolve_indexer_site(item),
                    keyword=keyword,
                    mtype=mtype,
                    page=page,
                ): item
                for item in indexers
            }
            for future in as_completed(futures):
                item = futures[future]
                label = str(item.get("origin_name") or item.get("name") or item.get("indexer_id"))
                try:
                    items = future.result() or []
                except Exception as e:
                    logger.error(
                        f"【{self.plugin_name}】{label} 检索异常：{str(e)}\n{traceback.format_exc()}"
                    )
                    details.append(f"{label} 异常")
                    continue
                results.extend(items)
                details.append(f"{label} {len(items)} 条")

        elapsed = int((time.monotonic() - started) * 1000)
        detail_text = "，".join(details)
        logger.info(
            f"【{self.plugin_name}】插件资源源检索完成：合计 {len(results)} 条，"
            f"耗时 {elapsed}ms；{detail_text}"
        )
        return results

    def __resolve_indexer_site(self, indexer: Dict[str, Any]) -> Dict[str, Any]:
        """
        取索引器对应的运行时站点信息，用于标注检索结果的归属站点。

        优先使用已注册到站点索引助手的条目，保证 site、site_name 等字段与站点体系一致；
        未注册或读取失败时回退到索引器快照本身。

        :param indexer: 索引器快照
        :return: 站点信息字典
        """
        domain = indexer.get("domain")
        site: Dict[str, Any] = {}
        if domain:
            try:
                registered = self.sites_helper.get_indexer(domain)
            except Exception as e:
                registered = None
                logger.warn(f"【{self.plugin_name}】读取站点索引 {domain} 失败：{str(e)}")
            if isinstance(registered, dict) and registered:
                site = dict(registered)
                site["indexer_id"] = indexer.get("indexer_id")
                site.setdefault("name", indexer.get("name"))
                site.setdefault("domain", domain)
        if not site:
            site = dict(indexer)
        # TorrentInfo.site 需要整型站点 ID：注册条目或快照携带的字符串 id 会让
        # Pydantic 校验失败并丢弃整批结果，缺整型 id 时回退快照 site_id 或数据库
        if not isinstance(site.get("id"), int):
            site["id"] = self.__resolve_site_id(indexer)
        return site

    def __resolve_site_id(self, indexer: Dict[str, Any]) -> Optional[int]:
        """
        解析索引器对应的整型站点 ID。

        :param indexer: 索引器快照
        :return: 站点 ID，无法确定时返回 None
        """
        site_id = indexer.get("site_id")
        if isinstance(site_id, int):
            return site_id
        domain = str(indexer.get("domain") or "")
        if domain:
            try:
                db_site = self.site_oper.get_by_domain(domain)
            except Exception as e:
                logger.warn(f"【{self.plugin_name}】查询站点 {domain} 失败：{str(e)}")
                db_site = None
            if db_site and getattr(db_site, "id", None):
                indexer["site_id"] = db_site.id
                return db_site.id
        return None

    def __filter_indexers_by_sites(self, indexers: List[Dict[str, Any]],
                                   sites: Optional[List[int]] = None) -> List[Dict[str, Any]]:
        """
        按宿主选中的站点 ID 过滤索引器，未指定选中项时返回全部。

        :param indexers: 索引器快照列表
        :param sites: 选中的站点 ID 列表，空表示不限制
        :return: 参与本次检索的索引器列表
        """
        selected = set(self.__normalize_site_ids(sites))
        if not selected:
            return indexers
        filtered = []
        for indexer in indexers:
            site_id = self.__resolve_site_id(indexer)
            if site_id is not None:
                indexer["site_id"] = site_id
            if site_id in selected:
                filtered.append(indexer)
        if len(filtered) != len(indexers):
            logger.info(
                f"【{self.plugin_name}】按选中站点过滤索引器：{len(indexers)} → {len(filtered)}"
            )
        return filtered

    def __query_jackett(self, indexer_id: str, site: dict, keyword: str = None,
                        mtype: Optional[MediaType] = None,
                        page: Optional[int] = 0) -> List[TorrentInfo]:
        """
        请求单个 Jackett 索引器的 Torznab 接口并解析为资源列表。

        :param indexer_id: Jackett 索引器 ID
        :param site: 用于标注检索结果的站点信息
        :param keyword: 搜索关键词
        :param mtype: 媒体类型
        :param page: 页码
        :return: 资源列表，失败时返回空列表
        """
        # 系统站点分类（cat）与 Torznab 分类体系不同，这里只按媒体类型映射
        params = [
            ("apikey", self._api_key),
            ("t", "search"),
            ("q", keyword or ""),
            ("cat", ",".join(str(item) for item in self.get_cat(mtype))),
            ("limit", self._result_num),
            ("offset", (page or 0) * self._result_num),
        ]
        api_url = f"{self._host}/api/v2.0/indexers/{indexer_id}/results/torznab/api?" \
                  f"{urlencode(params, quote_via=quote_plus)}"
        site_label = str(site.get("name") or indexer_id).replace(f"{self.plugin_name}-", "", 1)

        started = time.monotonic()
        try:
            logger.info(
                f"【{self.plugin_name}】开始检索索引器：{site_label}，关键词：{keyword}，"
                f"timeout={self._search_timeout}s"
            )
            res = RequestUtils(
                headers={"User-Agent": settings.USER_AGENT, "X-Api-Key": self._api_key},
                timeout=self._search_timeout,
            ).get_res(api_url)
            elapsed = int((time.monotonic() - started) * 1000)
            if not res:
                logger.warn(f"【{self.plugin_name}】{site_label} 检索无响应，耗时 {elapsed}ms")
                return []
            if res.status_code >= 400:
                logger.error(f"【{self.plugin_name}】{site_label} 检索失败：HTTP {res.status_code}")
                return []
            results = self.__parse_torznab(res.text, site=site, site_name=site_label)
            logger.info(
                f"【{self.plugin_name}】{site_label} 检索完成：{len(results)} 条，耗时 {elapsed}ms"
            )
            return results
        except Exception as e:
            logger.error(
                f"【{self.plugin_name}】{site_label} 检索出错：{str(e)}\n{traceback.format_exc()}"
            )
            return []

    async def async_search_torrents(self, site: Optional[dict] = None, keyword: str = None,
                                   mtype: Optional[MediaType] = None,
                                   page: Optional[int] = 0, **kwargs) -> List[TorrentInfo]:
        """
        异步检索资源。

        宿主异步链的 _async_call 对协程函数直接 await、不落线程池，因此本协程与权威帧
        _iter_torrent_events 同在事件循环线程，帧链完整：必须在进入线程池之前先把选中
        站点取出来，随调用显式下传，否则线程池里已看不到宿主栈帧。
        """
        if kwargs.get("sites") is None:
            extracted = self.__extract_selected_sites()
            if extracted is not None:
                kwargs["sites"] = extracted
        return await run_in_threadpool(
            self.search_torrents,
            site=site,
            keyword=keyword,
            mtype=mtype,
            page=page,
            **kwargs,
        )

    def refresh_torrents(self, site: Optional[dict] = None, keyword: str = None,
                         cat: str = None,
                         page: Optional[int] = 0, **kwargs) -> List[TorrentInfo]:
        """
        获取索引器最新种子，供订阅刷新（spider 模式）与站点资源浏览使用。

        MoviePilot V3 的刷新链路（ChainBase.refresh_torrents，入口是 app/chain/torrents.py
        的 browse / rss）恒以真实站点调用，宿主没有「插件资源源刷新」入口，因此这里只处理
        已托管的虚拟站点；空站点直接返回，不把刷新误当作插件资源源检索。

        Torznab 不分页浏览首页，这里只在第 0 页返回数据，避免订阅刷新重复请求。

        :param site: 站点信息
        :param keyword: 关键词，订阅刷新时为空表示取最新
        :param cat: 系统站点分类，Torznab 分类体系不同，忽略
        :param page: 页码
        :return: 资源列表
        """
        if not self.get_state() or not site:
            return []
        if not self.__is_managed_site(site):
            return []
        if (page or 0) > 0:
            return []
        return self.__search_managed_site(site=site, keyword=keyword, mtype=None, page=0)

    async def async_refresh_torrents(self, site: Optional[dict] = None, keyword: str = None,
                                     cat: str = None,
                                     page: Optional[int] = 0, **kwargs) -> List[TorrentInfo]:
        """
        异步获取索引器最新种子。

        宿主异步刷新（async_run_module）同样恒传真实站点，没有权威帧可回溯，
        本方法只是把同步实现交给线程池执行。
        """
        return await run_in_threadpool(
            self.refresh_torrents,
            site=site,
            keyword=keyword,
            cat=cat,
            page=page,
        )

    def __parse_torznab(self, content: str, site: dict, site_name: str) -> List[TorrentInfo]:
        """
        解析 Torznab XML 响应为种子列表。

        :param content: XML 文本
        :param site: 站点信息
        :param site_name: 展示用站点名称
        """
        results: List[TorrentInfo] = []
        if not content:
            return results
        try:
            root = ET.fromstring(content)
        except Exception as e:
            logger.warn(f"【{self.plugin_name}】{site.get('name')} 返回内容不是有效 XML：{str(e)}")
            return results

        # Jackett 一般部署在内网，取种直连，不使用代理
        site_proxy = bool(site.get("proxy"))

        for item in root.iter("item"):
            title = (item.findtext("title") or "").strip()
            if not title:
                continue
            attrs = self.__torznab_attrs(item)
            enclosure = self.__torrent_url(item, attrs)
            if not enclosure:
                continue
            seeders = self.__to_number(attrs.get("seeders"), 0)
            peers = attrs.get("peers")
            leechers = self.__to_number(attrs.get("leechers"), None)
            if leechers is None:
                total_peers = self.__to_number(peers, 0)
                leechers = max(0, total_peers - seeders) if total_peers else 0
            # RSS 条目允许重复出现 <category> 元素，逐个收集并去重
            categories = []
            for node in item.findall("category"):
                for text in (node.text or "").split(","):
                    text = text.strip()
                    if text and text not in categories:
                        categories.append(text)
            # MoviePilot V3 的 TorrentInfo 用 media_source/media_id 取代了 V2 的 imdbid 字段
            imdb_id = self.__parse_imdbid(attrs.get("imdb") or attrs.get("imdbid"))
            results.append(TorrentInfo(
                site=site.get("id"),
                site_name=site_name,
                site_cookie=site.get("cookie"),
                site_ua=site.get("ua") or settings.USER_AGENT,
                site_proxy=site_proxy,
                site_order=site.get("pri") or 0,
                site_downloader=site.get("downloader"),
                title=title,
                description=(item.findtext("description") or "").strip() or None,
                enclosure=enclosure,
                page_url=(item.findtext("comments") or item.findtext("guid") or "").strip() or None,
                size=self.__to_number(item.findtext("size") or attrs.get("size"), 0),
                seeders=seeders,
                peers=leechers,
                grabs=self.__to_number(attrs.get("grabs"), 0),
                pubdate=self.__parse_pubdate(item.findtext("pubDate")),
                media_source=MediaSource.IMDb if imdb_id else None,
                media_id=imdb_id,
                labels=categories,
                category=self.__infer_category(categories),
                downloadvolumefactor=self.__to_number(attrs.get("downloadvolumefactor"), 1.0),
                uploadvolumefactor=self.__to_number(attrs.get("uploadvolumefactor"), 1.0),
            ))
        return results

    @staticmethod
    def __torznab_attrs(item: ET.Element) -> Dict[str, str]:
        """
        提取 item 中的 torznab:attr 扩展属性。
        """
        attrs: Dict[str, str] = {}
        for child in item:
            tag = child.tag
            if tag.endswith("attr") or tag == f"{{{TORZNAB_NS}}}attr":
                name = child.get("name")
                if name:
                    attrs[str(name).lower()] = child.get("value")
        return attrs

    @staticmethod
    def __torrent_url(item: ET.Element, attrs: Dict[str, str]) -> Optional[str]:
        """
        获取种子下载地址，优先 enclosure，其次 link 与磁力链接。
        """
        enclosure = item.find("enclosure")
        if enclosure is not None and enclosure.get("url"):
            return enclosure.get("url")
        link = (item.findtext("link") or "").strip()
        if link:
            return link
        magnet = attrs.get("magneturl")
        return magnet or None

    @staticmethod
    def __to_number(value: Any, default: Any) -> Any:
        """
        将文本转换为数字，失败时返回默认值。
        """
        if value is None or value == "":
            return default
        try:
            number = float(value)
        except (TypeError, ValueError):
            return default
        return int(number) if float(number).is_integer() and not isinstance(default, float) else number

    @staticmethod
    def __parse_pubdate(value: Optional[str]) -> Optional[str]:
        """
        将 RFC822 发布时间转换为标准时间字符串。
        """
        text = (value or "").strip()
        if not text:
            return None
        try:
            return parsedate_to_datetime(text).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(text, fmt).strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue
        return text

    @staticmethod
    def __parse_imdbid(value: Any) -> Optional[str]:
        """
        规范化 IMDB ID，补全 tt 前缀。
        """
        text = str(value or "").strip()
        if not text:
            return None
        if text.startswith("tt"):
            return text
        if text.isdigit():
            return f"tt{int(text):07d}"
        return None

    @staticmethod
    def __infer_category(categories: List[str]) -> Optional[str]:
        """
        根据 Torznab 分类号推断媒体分类。
        """
        for text in categories:
            if not text.isdigit():
                continue
            code = int(text)
            if 2000 <= code < 3000:
                return MediaType.MOVIE.value
            if 3000 <= code < 4000:
                return MediaType.MUSIC.value
            if 5000 <= code < 6000:
                return MediaType.TV.value
        return None

    # ------------------------------------------------------------------ API

    def api_status(self) -> Dict[str, Any]:
        """
        返回当前桥接状态，供前端或外部查询。
        """
        return {
            "code": 0,
            "enabled": self._enabled,
            "host": self._host,
            "cron": self._cron,
            "last_sync": self.get_data("last_sync"),
            "indexer_count": len(self._indexers or []),
            "indexers": [item.get("name") for item in (self._indexers or [])],
            # 诊断：站点索引助手中的注册结构是否携带媒体分类声明（音乐站点列表依赖）
            "helper_category_declared": self.__helper_category_declared(),
            # 诊断：最近一次插件资源源检索的选中站点及其来源
            # explicit=宿主显式传入 / frame=从权威帧提取 / system=系统「搜索站点」/ unlimited=不限
            "selected_sites": {
                "source": self._last_sites_source,
                "frame_detected": self._last_frame_sites is not None,
                "frame_sites": self._last_frame_sites,
                "effective_sites": self._last_selected_sites,
            },
        }

    def __helper_category_declared(self) -> Optional[bool]:
        """
        检查站点索引助手中注册的索引器结构是否携带媒体分类声明。

        :return: 已声明返回 True，未声明返回 False，无法读取时返回 None
        """
        for indexer in self._indexers or []:
            domain = indexer.get("domain")
            if not domain:
                continue
            try:
                registered = self.sites_helper.get_indexer(domain)
            except Exception as e:
                logger.warn(f"【{self.plugin_name}】读取站点索引 {domain} 失败：{str(e)}")
                return None
            if isinstance(registered, dict):
                return bool(registered.get("category"))
            return False
        return None

    def api_test(self) -> Dict[str, Any]:
        """
        测试 Jackett 连通性并返回索引器数量。
        """
        if not self._host or not self._api_key:
            return {"code": 1, "message": "请先配置 Jackett 地址与 API Key"}
        indexers = self.__fetch_indexers()
        if indexers is None:
            return {"code": 1, "message": "连接失败，请检查地址、API Key 与管理密码"}
        selected = self.__apply_selection(indexers)
        return {"code": 0, "message": f"连接成功，已配置索引器 {len(selected)} 个"}

    def api_sync(self) -> Dict[str, Any]:
        """
        触发一次索引器同步。
        """
        if not self.get_state():
            return {"code": 1, "message": "插件未启用或配置不完整"}
        self.__start_sync_thread()
        return {"code": 0, "message": "已开始同步索引器"}

    # ------------------------------------------------------------------ 界面

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        返回插件配置表单与默认配置。
        """
        indexer_items = self._indexer_catalog or [
            {"title": item.get("origin_name") or item.get("name"), "value": item.get("indexer_id")}
            for item in (self._indexers or [])
        ]
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "div",
                        "props": {"class": "text-caption mb-1"},
                        "text": "基本设置"
                    },
                    {
                        "component": "VRow",
                        "props": {"dense": True},
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {"model": "enabled", "label": "启用插件"}
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {"model": "onlyonce", "label": "立即同步一次"}
                                }]
                            }
                        ]
                    },
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "info",
                            "variant": "tonal",
                            "density": "compact",
                            "class": "mb-2",
                            "text": "站点管理板块显示站点无法连通是正常现象，不用管。"
                                    "如果日志中提示【jackettBridge】索引器列表请求无响应，请检查地址与网络，"
                                    "请尝试将网络地址改为 MoviePilot 所在网段的地址，如：http://172.18.0.1:9117。"
                        }
                    },
                    {"component": "VDivider", "props": {"class": "my-3"}},
                    {
                        "component": "div",
                        "props": {"class": "text-caption mb-1"},
                        "text": "Jackett 连接"
                    },
                    {
                        "component": "VRow",
                        "props": {"dense": True},
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "host",
                                        "label": "Jackett 地址",
                                        "placeholder": "http://192.168.1.10:9117"
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "api_key",
                                        "label": "Jackett API Key",
                                        "placeholder": "Jackett 面板右上角 API Key"
                                    }
                                }]
                            }
                        ]
                    },
                    {
                        "component": "VRow",
                        "props": {"dense": True},
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "password",
                                        "label": "管理密码（可选）",
                                        "type": "password",
                                        "placeholder": "Jackett 设置了 Admin password 时填写"
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VCronField",
                                    "props": {"model": "cron", "label": "索引器同步周期"}
                                }]
                            }
                        ]
                    },
                    {"component": "VDivider", "props": {"class": "my-3"}},
                    {
                        "component": "div",
                        "props": {"class": "text-caption mb-1"},
                        "text": "检索参数"
                    },
                    {
                        "component": "VRow",
                        "props": {"dense": True},
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "search_timeout",
                                        "label": "检索超时（秒）",
                                        "type": "number",
                                        "placeholder": "30"
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "result_num",
                                        "label": "单索引器结果上限",
                                        "type": "number",
                                        "placeholder": "100"
                                    }
                                }]
                            }
                        ]
                    },
                    {
                        "component": "VRow",
                        "props": {"dense": True},
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [{
                                    "component": "VSelect",
                                    "props": {
                                        "model": "selected_indexers",
                                        "label": "桥接索引器（留空表示全部）",
                                        "multiple": True,
                                        "chips": True,
                                        "clearable": True,
                                        "items": indexer_items
                                    }
                                }]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "host": "",
            "api_key": "",
            "password": "",
            "cron": DEFAULT_CRON,
            "search_timeout": DEFAULT_SEARCH_TIMEOUT,
            "result_num": DEFAULT_RESULT_NUM,
            "selected_indexers": [],
        }

    def get_page(self) -> Optional[List[dict]]:
        """
        返回插件详情页面，展示已桥接的索引器。
        """
        indexers = self._indexers or []
        last_sync = self.get_data("last_sync") or "尚未同步"
        if not indexers:
            return [{
                "component": "VAlert",
                "props": {
                    "type": "warning",
                    "variant": "tonal",
                    "text": f"暂无桥接索引器，请检查配置后开启「立即同步一次」。最近同步：{last_sync}"
                }
            }]
        rows = []
        for indexer in indexers:
            rows.append({
                "component": "tr",
                "content": [
                    {"component": "td", "text": indexer.get("origin_name") or indexer.get("name")},
                    {"component": "td", "text": indexer.get("indexer_id")},
                    {"component": "td", "text": "公开" if indexer.get("public") else indexer.get("privacy")},
                    {"component": "td", "text": indexer.get("domain")},
                ]
            })
        return [
            {
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "variant": "tonal",
                    "density": "compact",
                    "class": "mb-3",
                    "text": f"Jackett：{self._host or '未配置'}　索引器：{len(indexers)} 个　最近同步：{last_sync}"
                }
            },
            {
                "component": "VTable",
                "props": {"hover": True, "density": "compact"},
                "content": [
                    {
                        "component": "thead",
                        "content": [{
                            "component": "tr",
                            "content": [
                                {"component": "th", "text": "索引器"},
                                {"component": "th", "text": "Jackett ID"},
                                {"component": "th", "text": "类型"},
                                {"component": "th", "text": "虚拟域名"},
                            ]
                        }]
                    },
                    {"component": "tbody", "content": rows}
                ]
            }
        ]
