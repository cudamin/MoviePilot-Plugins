import threading
from typing import Any, Dict, List, Optional, Tuple

from app.chain.subscribe import SubscribeChain
from app.core.event import Event as MPEvent
from app.core.event import eventmanager
from app.db.subscribe_oper import SubscribeOper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType


class SubscribeNoAutoSearch(_PluginBase):
    """新增订阅不自动搜索插件。"""

    plugin_name = "新增订阅不自动搜索"
    plugin_desc = "添加订阅后阻止新增订阅自动搜索，可选定时全站搜索补全订阅漏掉的资源。"
    plugin_icon = "pause.png"
    plugin_version = "1.3.0"
    plugin_label = "订阅管理"
    plugin_author = "local"
    plugin_config_prefix = "subscribenoautosearch_"
    plugin_order = 19
    auth_level = 1

    _enabled = False
    _protect_new_subscribe = True
    _timed_full_search = False
    _search_interval = 24
    _run_once = False
    _scheduler_thread: Optional[threading.Thread] = None
    _scheduler_stop_event: Optional[threading.Event] = None
    _search_running = False

    def init_plugin(self, config: dict = None) -> None:
        """根据插件配置初始化运行状态。"""
        self.stop_service()
        self._enabled = False
        self._protect_new_subscribe = True
        self._timed_full_search = False
        self._search_interval = 24
        self._run_once = False
        self._search_running = False

        if not config:
            return

        self._enabled = bool(config.get("enabled"))
        self._protect_new_subscribe = bool(config.get("protect_new_subscribe", True))
        self._timed_full_search = bool(config.get("timed_full_search", False))
        self._search_interval = self._safe_int(config.get("search_interval"), 24, 1)
        self._run_once = bool(config.get("run_once", False))

        if self._enabled and self._timed_full_search:
            self._start_scheduler()

        if self._enabled and self._run_once:
            self._run_once = False
            self.update_config(self._current_config())
            self._start_search_thread(reason="手动立即搜索")

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
                                            "model": "protect_new_subscribe",
                                            "label": "新增订阅不自动搜索",
                                            "hint": "启用后，新增订阅会直接进入订阅中状态，跳过系统新增订阅搜索；RSS 订阅下载和手动搜索不受影响",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
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
                                            "model": "timed_full_search",
                                            "label": "订阅定时全站搜索",
                                            "hint": "每隔指定时间搜索所有订阅中/待定订阅，用于补全 RSS 或新增订阅可能漏掉的资源；不影响新增订阅不自动搜索开关",
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
                                            "model": "search_interval",
                                            "label": "全站搜索间隔（小时）",
                                            "type": "number",
                                            "min": 1,
                                            "hint": "建议不要过短，搜索会调用所有订阅配置的站点范围",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "run_once",
                                            "label": "保存后立即执行一次订阅全站搜索",
                                            "hint": "开关保存后会自动复位；用于临时补全订阅漏掉的资源",
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
                                            "text": "两个功能互不影响：新增订阅不自动搜索只处理新建订阅；订阅定时全站搜索只定时搜索已启用/待定订阅，用于后续补漏。",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], self._default_config()

    def get_page(self):
        """返回插件详情页面。"""
        return [
            {
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "text": f"新增订阅不自动搜索：{'开启' if self._protect_new_subscribe else '关闭'}；订阅定时全站搜索：{'开启' if self._timed_full_search else '关闭'}，间隔 {self._search_interval} 小时。",
                },
            }
        ]

    def stop_service(self) -> None:
        """停止插件后台服务。"""
        if self._scheduler_stop_event:
            self._scheduler_stop_event.set()
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join(timeout=3)
        self._scheduler_thread = None
        self._scheduler_stop_event = None

    @eventmanager.register(EventType.SubscribeAdded)
    def on_subscribe_added(self, event: MPEvent) -> None:
        """处理订阅添加事件。"""
        if not self._enabled or not self._protect_new_subscribe:
            return
        subscribe_id = self._get_subscribe_id(event)
        if not subscribe_id:
            return
        self._activate_new_subscribe(subscribe_id)

    def _start_scheduler(self) -> None:
        """启动订阅定时全站搜索线程。"""
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            return
        self._scheduler_stop_event = threading.Event()
        self._scheduler_thread = threading.Thread(
            target=self._scheduler_loop,
            daemon=True,
            name="SubscribeNoAutoSearchFullSearch",
        )
        self._scheduler_thread.start()

    def _scheduler_loop(self) -> None:
        """按配置间隔循环执行订阅全站搜索。"""
        assert self._scheduler_stop_event is not None
        interval_seconds = max(self._search_interval, 1) * 3600
        logger.info(f"订阅定时全站搜索已启动，间隔 {self._search_interval} 小时")
        while not self._scheduler_stop_event.wait(interval_seconds):
            self._run_full_search(reason="定时全站搜索")

    def _start_search_thread(self, reason: str) -> None:
        """启动一次后台订阅搜索。"""
        threading.Thread(
            target=self._run_full_search,
            kwargs={"reason": reason},
            daemon=True,
            name="SubscribeNoAutoSearchRunOnce",
        ).start()

    def _run_full_search(self, reason: str) -> None:
        """执行一次订阅搜索补全。"""
        if self._search_running:
            logger.info(f"{reason} 已有任务运行中，跳过本次执行")
            return
        self._search_running = True
        try:
            logger.info(f"开始{reason}，范围：订阅中/待定订阅")
            SubscribeChain().search(state="R,P", manual=False)
            logger.info(f"{reason}完成")
        except Exception as err:
            logger.error(f"{reason}失败: {err}")
        finally:
            self._search_running = False

    def _current_config(self) -> Dict[str, Any]:
        """返回当前插件配置。"""
        return {
            "enabled": self._enabled,
            "protect_new_subscribe": self._protect_new_subscribe,
            "timed_full_search": self._timed_full_search,
            "search_interval": self._search_interval,
            "run_once": self._run_once,
        }

    @staticmethod
    def _default_config() -> Dict[str, Any]:
        """返回默认插件配置。"""
        return {
            "enabled": False,
            "protect_new_subscribe": True,
            "timed_full_search": False,
            "search_interval": 24,
            "run_once": False,
        }

    @staticmethod
    def _safe_int(value: Any, default: int, minimum: int) -> int:
        """安全转换整数配置。"""
        try:
            result = int(value)
        except Exception:
            return default
        return max(result, minimum)

    @staticmethod
    def _get_subscribe_id(event: MPEvent) -> int:
        """从事件中获取订阅 ID。"""
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
