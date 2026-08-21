# -*- coding: utf-8 -*-
import copy
import re
import threading
import time
import traceback
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, quote_plus, urlparse

import requests
from apscheduler.triggers.cron import CronTrigger
from fastapi.concurrency import run_in_threadpool

from app.core.config import settings
from app.core.context import TorrentInfo
from app.db.models.site import Site
from app.helper.sites import SitesHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, MediaType, SystemConfigKey
from app.utils.http import RequestUtils

# Torznab 命名空间
TORZNAB_NS = "http://torznab.com/schemas/2015/feed"
# 默认同步周期：每天零点
DEFAULT_CRON = "0 0 * * *"
# 默认检索超时（秒）
DEFAULT_SEARCH_TIMEOUT = 30
# 默认单个索引器返回条数
DEFAULT_RESULT_NUM = 100


class Jackett(_PluginBase):
    """
    Jackett 索引器桥接插件。

    参考 nas-tools 的 Jackett 实现：登录 Jackett 拉取已配置的索引器，注册为 MoviePilot
    虚拟站点，并通过 Torznab API 完成资源检索。
    """

    # 插件名称
    plugin_name = "Jackett"
    # 插件描述
    plugin_desc = "将 Jackett 中已配置的索引器接入 MoviePilot 搜索与订阅。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/Jackett_A.png"
    # 插件版本
    plugin_version = "1.0.0"
    # 插件标签
    plugin_label = "站点"
    # 插件作者
    plugin_author = "cudamin"
    # 作者主页
    author_url = "https://github.com/cudamin"
    # 插件配置项ID前缀
    plugin_config_prefix = "jackett_"
    # 加载顺序
    plugin_order = 16
    # 可使用的用户级别
    auth_level = 1

    # 虚拟站点域名前缀与后缀
    domain_prefix = "jackett-"
    domain_suffix = "extend"

    # 运行时状态默认值
    _enabled = False
    _proxy = False
    _onlyonce = False
    _sync_search_sites = True
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

    def init_plugin(self, config: dict = None) -> None:
        """
        根据插件配置初始化运行状态，并在需要时后台同步索引器。
        """
        self.sites_helper = SitesHelper()
        self._enabled = False
        self._proxy = False
        self._onlyonce = False
        self._sync_search_sites = True
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
        self._sync_lock = threading.Lock()

        # 恢复上次同步的索引器快照，避免重启后检索失效
        saved = self.get_data("indexers") or []
        if isinstance(saved, list):
            self._indexers = [item for item in saved if isinstance(item, dict)]

        if not config:
            return

        saved_config = self.get_config() or {}
        self._enabled = bool(config.get("enabled"))
        self._proxy = bool(config.get("proxy"))
        self._onlyonce = bool(config.get("onlyonce"))
        self._sync_search_sites = bool(config.get("sync_search_sites", True))
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
            # 使用本地快照先把虚拟站点注册回来
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
            "id": "JackettSyncIndexers",
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
        """
        return None

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
        持久化当前插件配置。
        """
        self.update_config({
            "enabled": self._enabled,
            "proxy": self._proxy,
            "onlyonce": self._onlyonce,
            "sync_search_sites": self._sync_search_sites,
            "host": self._host,
            "api_key": self._api_key,
            "password": self._password,
            "cron": self._cron,
            "search_timeout": self._search_timeout,
            "result_num": self._result_num,
            "selected_indexers": self._selected_indexers,
            "indexer_catalog": self._indexer_catalog,
        })

    # ------------------------------------------------------------------ 同步

    def __start_sync_thread(self, restore_only: bool = False) -> None:
        """
        在后台线程中同步索引器，避免阻塞插件加载。
        """
        threading.Thread(
            target=self.sync_indexers,
            kwargs={"restore_only": restore_only},
            daemon=True,
            name="JackettSyncIndexers",
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
                self.__update_config()

            if not self._indexers:
                logger.info(f"【{self.plugin_name}】没有可用索引器")

            registered, updated = self.__sync_helper_indexers()
            site_ids, removed_site_ids = self.__sync_site_records()
            if self._sync_search_sites:
                self.__sync_search_sites(site_ids, removed_site_ids)
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

    def __proxies(self) -> Optional[dict]:
        """
        按配置返回代理设置。
        """
        return settings.PROXY if self._proxy else None

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
                params={"password": self._password},
                proxies=self.__proxies(),
            )
            if res and session.cookies:
                return session.cookies.get_dict()
            logger.warn(f"【{self.plugin_name}】Jackett 面板登录失败，未获取到 Cookie")
        except Exception as e:
            logger.warn(f"【{self.plugin_name}】Jackett 面板登录异常：{str(e)}")
        return None

    def get_indexers(self) -> Optional[List[Dict[str, Any]]]:
        """
        获取 Jackett 中已配置的索引器并转换为虚拟站点结构。

        :return: 索引器列表，请求失败时返回 None
        """
        self._indexers_authoritative = False
        cookies = self.__login_cookies()
        try:
            res = RequestUtils(
                headers=self.__headers(),
                cookies=cookies,
                timeout=15,
            ).get_res(
                f"{self._host}/api/v2.0/indexers?configured=true",
                params={"apikey": self._api_key},
                proxies=self.__proxies(),
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
        try:
            data = res.json()
        except Exception as e:
            logger.error(f"【{self.plugin_name}】索引器列表返回数据解析失败：{str(e)}")
            return None
        if not isinstance(data, list):
            logger.error(f"【{self.plugin_name}】索引器列表返回数据格式异常")
            return None

        self._indexers_authoritative = True
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

        self._indexer_catalog = [
            {"title": item.get("origin_name") or item.get("name"), "value": item.get("indexer_id")}
            for item in indexers
        ]
        logger.info(
            f"【{self.plugin_name}】Jackett 返回 {len(indexers)} 个已配置索引器，跳过未配置 {skipped} 个"
        )
        return self.__apply_selection(indexers)

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
        # parser/plugin 标记用于识别本插件托管的虚拟站点；空 search.paths 让系统蜘蛛尽早放弃，
        # 避免对虚拟域名做 DNS/HTTP 重试。
        return {
            "id": f"{self.plugin_name}-{indexer_id}",
            "name": f"{self.plugin_name}-{indexer_name}",
            "origin_name": indexer_name,
            "indexer_id": indexer_id,
            "url": f"{self._host}/api/v2.0/indexers/{indexer_id}/results/torznab/",
            "domain": domain,
            "public": privacy == "public",
            "privacy": privacy,
            "proxy": self._proxy,
            "result_num": self._result_num,
            "timeout": 5,
            "parser": self.plugin_name,
            "plugin": self.plugin_name,
            "search": {"paths": []},
            "browse": {"path": ""},
            "torrents": {"list": {"selector": ""}, "fields": {}},
        }

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
        for key in ("id", "name", "url", "domain", "public", "privacy", "proxy",
                    "parser", "plugin", "result_num", "timeout"):
            if exists.get(key) != indexer.get(key):
                return False
        return True

    def __get_managed_site_records(self) -> List[Site]:
        """
        读取数据库中由本插件托管的站点记录。
        """
        try:
            sites = Site.list_order_by_pri(None) or []
        except Exception as e:
            logger.warn(f"【{self.plugin_name}】读取站点列表失败，跳过旧站点清理：{str(e)}")
            return []
        return [site for site in sites if self.__is_managed_domain(getattr(site, "domain", ""))]

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
                "proxy": 1 if self._proxy else 0,
                "render": 0,
                "timeout": 5,
                "is_active": True,
                "note": {
                    "managed_by": self.plugin_name,
                    "indexer_id": indexer.get("indexer_id"),
                    "privacy": indexer.get("privacy") or "unknown",
                },
            }
            site = Site.get_by_domain(None, domain)
            if not site:
                Site(**payload).create(None)
                site = Site.get_by_domain(None, domain)
                created += 1
            else:
                changes = {k: v for k, v in payload.items() if getattr(site, k, None) != v}
                if changes:
                    site.update(None, changes)
                    site = Site.get_by_domain(None, domain)
                    updated += 1
            if site and site.id:
                site_ids.append(site.id)

        if self._indexers_authoritative:
            for site in self.__get_managed_site_records():
                site_id = getattr(site, "id", None)
                if site_id and getattr(site, "domain", "") not in current_domains:
                    Site.delete(None, site_id)
                    removed_site_ids.append(site_id)
                    removed += 1

        if created or updated or removed:
            self.eventmanager.send_event(EventType.SiteUpdated, {"plugin_id": self.plugin_name})
            logger.info(
                f"【{self.plugin_name}】同步站点记录：新增 {created} 个、更新 {updated} 个、清理 {removed} 个"
            )
        return site_ids, removed_site_ids

    def __sync_search_sites(self, site_ids: List[int], removed_site_ids: List[int]) -> None:
        """
        将虚拟站点同步到搜索站点范围，并清理已删除的站点。
        """
        selected = self.systemconfig.get(SystemConfigKey.IndexerSites) or []
        if not selected:
            # 搜索范围为空表示使用全部站点，无需处理
            return
        removed_keys = {str(site_id) for site_id in removed_site_ids}
        cleaned = [site_id for site_id in selected if str(site_id) not in removed_keys]
        exists_keys = {str(site_id) for site_id in cleaned}
        missing = [site_id for site_id in site_ids if str(site_id) not in exists_keys]
        if not missing and cleaned == selected:
            return
        self.systemconfig.set(SystemConfigKey.IndexerSites, cleaned + missing)
        logger.info(
            f"【{self.plugin_name}】已同步 {len(missing)} 个站点到搜索范围，清理 {len(removed_keys)} 个失效站点"
        )

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
        获取 Torznab 分类，电影 2000、剧集 5000。
        """
        if mtype == MediaType.MOVIE:
            return [2000]
        if mtype == MediaType.TV:
            return [5000]
        return [2000, 5000]

    def search_torrents(self, site: dict, keyword: str = None, mtype: Optional[MediaType] = None,
                        page: Optional[int] = 0, **kwargs) -> List[TorrentInfo]:
        """
        通过 Jackett Torznab 接口检索单个索引器。

        :param site: 站点信息
        :param keyword: 搜索关键词
        :param mtype: 媒体类型
        :param page: 页码
        :return: 资源列表
        """
        results: List[TorrentInfo] = []
        if not site or not self.__is_managed_site(site):
            return results
        if not self.get_state():
            return results

        indexer_id = self.__get_indexer_id(site)
        if not indexer_id:
            logger.warn(
                f"【{self.plugin_name}】无法解析索引器 ID，跳过站点：{site.get('name')}"
                f"（domain={site.get('domain')}）"
            )
            return results

        # 已不在当前桥接列表中的残留站点直接跳过
        if self._indexers:
            current_ids = {str(item.get("indexer_id")) for item in self._indexers}
            if str(indexer_id) not in current_ids:
                logger.warn(
                    f"【{self.plugin_name}】索引器 {indexer_id} 已不在桥接列表，跳过残留站点：{site.get('name')}"
                )
                return results

        site_name = str(site.get("name") or "").replace(f"{self.plugin_name}-", "", 1)
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

        started = time.monotonic()
        try:
            logger.info(
                f"【{self.plugin_name}】开始检索索引器：{site.get('name')}，关键词：{keyword}，"
                f"timeout={self._search_timeout}s"
            )
            res = RequestUtils(
                headers={"User-Agent": settings.USER_AGENT, "X-Api-Key": self._api_key},
                timeout=self._search_timeout,
            ).get_res(api_url, proxies=self.__proxies())
            elapsed = int((time.monotonic() - started) * 1000)
            if not res:
                logger.warn(f"【{self.plugin_name}】{site.get('name')} 检索无响应，耗时 {elapsed}ms")
                return results
            if res.status_code >= 400:
                logger.error(f"【{self.plugin_name}】{site.get('name')} 检索失败：HTTP {res.status_code}")
                return results
            results = self.__parse_torznab(res.text, site=site, site_name=site_name)
            logger.info(
                f"【{self.plugin_name}】{site.get('name')} 检索完成：{len(results)} 条，耗时 {elapsed}ms"
            )
        except Exception as e:
            logger.error(f"【{self.plugin_name}】{site.get('name')} 检索出错：{str(e)}\n{traceback.format_exc()}")
        return results

    async def async_search_torrents(self, site: dict, keyword: str = None, mtype: Optional[MediaType] = None,
                                   page: Optional[int] = 0, **kwargs) -> List[TorrentInfo]:
        """
        异步检索单个索引器，内部转为线程池执行同步实现。
        """
        return await run_in_threadpool(
            self.search_torrents,
            site=site,
            keyword=keyword,
            mtype=mtype,
            page=page,
            **kwargs,
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

        site_proxy = site.get("proxy")
        site_proxy = self._proxy if site_proxy is None else bool(site_proxy)

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
            categories = [text.strip() for text in
                          (item.findtext("category") or "").split(",") if text.strip()]
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
                imdbid=self.__parse_imdbid(attrs.get("imdb") or attrs.get("imdbid")),
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
        }

    def api_test(self) -> Dict[str, Any]:
        """
        测试 Jackett 连通性并返回索引器数量。
        """
        if not self._host or not self._api_key:
            return {"code": 1, "message": "请先配置 Jackett 地址与 API Key"}
        indexers = self.get_indexers()
        if indexers is None:
            return {"code": 1, "message": "连接失败，请检查地址、API Key 与管理密码"}
        return {"code": 0, "message": f"连接成功，已配置索引器 {len(indexers)} 个"}

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
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {"model": "enabled", "label": "启用插件"}
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {"model": "proxy", "label": "使用代理"}
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {"model": "sync_search_sites", "label": "同步至搜索范围"}
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {"model": "onlyonce", "label": "立即同步一次"}
                                }]
                            }
                        ]
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
                                        "label": "API Key",
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
                    },
                    {
                        "component": "VRow",
                        "props": {"dense": True},
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [{
                                    "component": "VAlert",
                                    "props": {
                                        "type": "info",
                                        "variant": "tonal",
                                        "density": "compact",
                                        "text": "同步后每个 Jackett 索引器会生成一个虚拟站点（jackett-xxx.extend），"
                                                "检索走 Torznab 接口。索引器列表需要管理密码时请填写，否则接口会返回 401。"
                                    }
                                }]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "proxy": False,
            "onlyonce": False,
            "sync_search_sites": True,
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
