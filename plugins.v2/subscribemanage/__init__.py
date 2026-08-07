import re
import threading
import time
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from app.chain.download import DownloadChain
from app.chain.subscribe import SubscribeChain
from app.core.config import settings, global_vars
from app.core.context import Context, TorrentInfo
from app.core.event import Event as MPEvent
from app.core.event import eventmanager
from app.core.metainfo import MetaInfo
from app.db.subscribe_oper import SubscribeOper
from app.helper.rss import RssHelper
from app.helper.sites import SitesHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, SystemConfigKey, MediaType
from app.utils.string import StringUtils


class SubscribeManage(_PluginBase):
    """订阅管理插件。"""

    plugin_name = "订阅管理"
    plugin_desc = "订阅站点RSS刷新时先按包含/排除正则过滤，再参与TMDB识别；新增订阅不自动搜索。"
    plugin_icon = "subscribe.png"
    plugin_version = "1.0.0"
    plugin_label = "订阅管理"
    plugin_author = "tafei"
    author_url = "https://github.com/cudamin"
    plugin_config_prefix = "subscribemanage_"
    plugin_order = 19
    auth_level = 1

    _enabled: bool = False
    _onlyonce: bool = False
    _interval: int = 30
    _include: str = ""
    _exclude: str = ""
    _action: str = "subscribe"

    def init_plugin(self, config: dict = None) -> None:
        """根据插件配置初始化运行状态。"""
        if config:
            self._enabled = bool(config.get("enabled")) or False
            self._onlyonce = bool(config.get("only_once")) or False
            try:
                self._interval = int(config.get("interval") or 30)
            except Exception:
                self._interval = 30
            self._include = (config.get("include") or "").strip()
            self._exclude = (config.get("exclude") or "").strip()
            self._action = config.get("action") or "subscribe"
        else:
            self._enabled = False

        if self._onlyonce:
            self._onlyonce = False
            self.__update_config()
            self._trigger_once()

    def get_state(self) -> bool:
        """获取插件启用状态。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """返回插件远程命令列表。"""
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """返回插件 API 列表。"""
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        """注册插件定时服务。"""
        if self._enabled:
            return [{
                "id": "SubscribeManageRsRefresh",
                "name": "订阅RSS过滤刷新",
                "trigger": "interval",
                "func": self.__refresh,
                "kwargs": {"minutes": self._interval},
            }]
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """返回插件配置表单与默认配置。"""
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                            "hint": "启用后按刷新周期读取订阅站点RSS，先按包含/排除正则过滤，再进行TMDB识别与订阅/下载",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "only_once",
                                            "label": "立即刷新一次",
                                            "hint": "保存后立即执行一次RSS过滤刷新",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "interval",
                                            "label": "刷新间隔(分钟)",
                                            "type": "number",
                                            "placeholder": "默认30分钟",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "action",
                                            "label": "过滤命中后的动作",
                                            "items": [
                                                {"title": "订阅", "value": "subscribe"},
                                                {"title": "直接下载", "value": "download"},
                                            ],
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "include",
                                            "label": "包含",
                                            "placeholder": "支持正则表达式，如：^[A-Z]|蓝光",
                                            "hint": "RSS报文标题+副标题命中该正则才继续处理，留空不过滤",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "exclude",
                                            "label": "排除",
                                            "placeholder": "支持正则表达式，如：抢先|WEB-DL",
                                            "hint": "RSS报文标题+副标题命中该正则直接丢弃，留空不过滤",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "text": "插件按周期读取「订阅设置 -> RSS订阅站点」配置的站点RSS，先执行包含/排除正则过滤，过滤掉的报文不参与TMDB识别、不进入缓存；命中后才进行TMDB识别并执行所选动作。新增订阅不会自动搜索资源，改为由本插件RSS驱动，避免重复下载。",
                                        },
                                    }
                                ],
                            },
                        ],
                    }
                ],
            }
        ], {
            "enabled": False,
            "only_once": False,
            "interval": 30,
            "include": "",
            "exclude": "",
            "action": "subscribe",
        }

    def get_page(self):
        """返回插件详情页面。"""
        history = self.get_data("history") or []
        if not history:
            return [
                {
                    "component": "div",
                    "text": "暂无刷新记录",
                    "props": {"class": "text-center"},
                }
            ]
        history = sorted(history, key=lambda x: x.get("time", ""), reverse=True)
        rows = []
        for item in history[:50]:
            rows.append(
                {
                    "component": "VList",
                    "content": [
                        {
                            "component": "VListItem",
                            "props": {"title": item.get("title") or ""},
                            "content": [
                                {
                                    "component": "VListItemSubtitle",
                                    "props": {
                                        "text": f"类型：{item.get('mtype', '')}  时间：{item.get('time', '')}"
                                    },
                                }
                            ],
                        }
                    ],
                }
            )
        return [{"component": "div", "content": rows}]

    def stop_service(self) -> None:
        """停止插件。"""
        return None

    @eventmanager.register(EventType.SubscribeAdded)
    def on_subscribe_added(self, event: MPEvent) -> None:
        """处理订阅添加事件，新增订阅不自动搜索。"""
        if not self._enabled:
            return
        subscribe_id = self._get_subscribe_id(event)
        if not subscribe_id:
            return
        self._activate_new_subscribe(subscribe_id)

    @staticmethod
    def _get_subscribe_id(event: MPEvent) -> int:
        """从事件中获取订阅ID。"""
        event_data = event.event_data or {}
        try:
            return int(event_data.get("subscribe_id") or 0)
        except Exception:
            return 0

    @staticmethod
    def _activate_new_subscribe(subscribe_id: int) -> None:
        """将新建订阅状态改为启用状态。"""
        try:
            sub_oper = SubscribeOper()
            subscribe = sub_oper.get(subscribe_id)
            if not subscribe or subscribe.state != "N":
                return
            sub_oper.update(subscribe_id, {"state": "R"})
            subscribe_name = subscribe.name or f"订阅 #{subscribe_id}"
            logger.info(f"新增订阅「{subscribe_name}」(ID={subscribe_id}) 已设为启用状态，跳过自动搜索")
        except Exception as err:
            logger.error(f"修改订阅 {subscribe_id} 状态失败: {err}")

    def __refresh(self) -> None:
        """刷新订阅站点RSS，先包含/排除过滤再进行TMDB识别。"""
        if not self._enabled:
            return
        try:
            subscribechain = SubscribeChain()
            # 读取系统配置的订阅站点
            sites = self.systemconfig.get(SystemConfigKey.RssSites) or []
            if not sites:
                sites = subscribechain.get_subscribed_sites() or []
            if not sites:
                logger.warn("订阅管理：未配置订阅站点，跳过刷新")
                return
            # 已处理记录，避免重复
            history = self.get_data("history") or []
            for indexer in SitesHelper().get_indexers():
                if global_vars.is_system_stopped:
                    break
                if not indexer or indexer.get("id") not in sites:
                    continue
                if not indexer.get("rss"):
                    logger.warn(f"订阅管理：站点 {indexer.get('name')} 未配置RSS地址")
                    continue
                domain = StringUtils.get_url_domain(indexer.get("domain"))
                logger.info(f"订阅管理：开始刷新站点 {indexer.get('name')} RSS ...")
                results = RssHelper().parse(
                    indexer.get("rss"),
                    True if indexer.get("proxy") else False,
                    timeout=int(indexer.get("timeout") or 30),
                    ua=indexer.get("ua") if indexer.get("ua") else None,
                )
                if not results:
                    logger.warn(f"订阅管理：站点 {indexer.get('name')} 未获取到RSS数据")
                    continue
                for item in results:
                    if global_vars.is_system_stopped:
                        break
                    try:
                        title = item.get("title") or ""
                        description = item.get("description") or ""
                        if not title:
                            continue
                        if title in [h.get("key") for h in history]:
                            continue
                        # 先过滤：命中排除或不满足包含则不识别、不缓存
                        if not self.__passes_filter(title, description):
                            logger.info(f"订阅管理：RSS过滤命中 {title}，跳过TMDB识别")
                            continue
                        # 识别元数据
                        meta = MetaInfo(title=title, subtitle=description)
                        if not meta.name:
                            logger.info(f"订阅管理：{title} 未识别到有效名称")
                            continue
                        # TMDB识别
                        mediainfo = self.chain.recognize_media(meta=meta)
                        if not mediainfo:
                            logger.warn(f"订阅管理：{title} 未识别到媒体信息")
                            continue
                        torrentinfo = TorrentInfo(
                            site=indexer.get("id"),
                            site_name=indexer.get("name"),
                            site_cookie=indexer.get("cookie"),
                            site_ua=indexer.get("ua") or settings.USER_AGENT,
                            site_proxy=indexer.get("proxy"),
                            site_order=indexer.get("pri") or 0,
                            site_downloader=indexer.get("downloader"),
                            title=title,
                            description=description,
                            enclosure=item.get("enclosure"),
                            page_url=item.get("link"),
                            size=item.get("size"),
                            pubdate=item["pubdate"].strftime("%Y-%m-%d %H:%M:%S") if item.get("pubdate") else None,
                        )
                        if self._action == "download":
                            result = DownloadChain().download_single(
                                context=Context(
                                    meta_info=meta,
                                    media_info=mediainfo,
                                    torrent_info=torrentinfo,
                                    resource_source="rss",
                                ),
                                username="订阅管理",
                            )
                            if not result:
                                logger.error(f"订阅管理：{title} 下载失败")
                                continue
                        else:
                            if subscribechain.exists(mediainfo=mediainfo, meta=meta):
                                logger.info(f"订阅管理：{mediainfo.title_year} 正在订阅中")
                                continue
                            subscribechain.add(
                                title=mediainfo.title,
                                year=mediainfo.year,
                                mtype=mediainfo.type if isinstance(mediainfo.type, MediaType) else None,
                                tmdbid=mediainfo.tmdb_id or None,
                                season=meta.begin_season,
                                exist_ok=True,
                                username="订阅管理",
                            )
                        # 记录历史
                        history.append({
                            "key": title,
                            "title": f"{mediainfo.title} {meta.season or ''}".strip(),
                            "mtype": "电影" if mediainfo.type == MediaType.MOVIE else "电视剧",
                            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        })
                    except Exception as err:
                        logger.error(f"订阅管理：处理RSS数据出错：{err} - {traceback.format_exc()}")
                logger.info(f"订阅管理：站点 {indexer.get('name')}({domain}) 刷新完成")
            self.save_data("history", history[-500:])
        except Exception as err:
            logger.error(f"订阅管理：RSS刷新失败: {err} - {traceback.format_exc()}")

    def __passes_filter(self, title: str, description: str) -> bool:
        """包含/排除正则过滤，返回False表示丢弃该报文。"""
        text = f"{title} {description}"
        if self._include:
            try:
                if not re.search(self._include, text, re.IGNORECASE):
                    return False
            except re.error as err:
                logger.error(f"订阅管理：包含正则错误：{err}")
        if self._exclude:
            try:
                if re.search(self._exclude, text, re.IGNORECASE):
                    return False
            except re.error as err:
                logger.error(f"订阅管理：排除正则错误：{err}")
        return True

    def __update_config(self) -> None:
        """更新配置。"""
        self.update_config({
            "enabled": self._enabled,
            "only_once": False,
            "interval": self._interval,
            "include": self._include,
            "exclude": self._exclude,
            "action": self._action,
        })

    def _trigger_once(self) -> None:
        """立即执行一次RSS过滤刷新。"""
        if not self._enabled:
            logger.info("订阅管理：插件未启用，跳过立即刷新")
            return

        def _run():
            time.sleep(3)
            self.__refresh()

        threading.Thread(target=_run, name="SubscribeManageOnce", daemon=True).start()
        logger.info("订阅管理：立即刷新任务已启动")
