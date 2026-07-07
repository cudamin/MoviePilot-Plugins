from typing import Any, Dict, List, Tuple

from app.core.event import Event as MPEvent
from app.core.event import eventmanager
from app.db.subscribe_oper import SubscribeOper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType


class SubscribeNoAutoSearch(_PluginBase):
    """新增订阅不自动搜索插件。"""

    plugin_name = "新增订阅不自动搜索"
    plugin_desc = "添加订阅后阻止自动搜索资源，RSS 订阅下载和手动搜索不受影响。"
    plugin_icon = "pause.png"
    plugin_version = "1.2.0"
    plugin_label = "订阅管理"
    plugin_author = "cudamin"
    author_url = "https://github.com/cudamin"
    plugin_config_prefix = "subscribenoautosearch_"
    plugin_order = 19
    auth_level = 2

    _enabled = False

    def init_plugin(self, config: dict = None) -> None:
        """根据插件配置初始化运行状态。"""
        self._enabled = bool(config.get("enabled")) if config else False

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
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                            "hint": "启用后，新增订阅不会自动搜索资源，RSS 订阅下载和手动搜索不受影响",
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
                                            "text": "启用后新增的订阅会直接进入启用状态，不会被系统定时任务自动搜索。RSS 订阅刷新仍会正常匹配下载，也可在订阅列表中手动搜索。已存在的订阅不受影响。",
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
        }

    def get_page(self):
        """返回插件详情页面。"""
        pass

    def stop_service(self) -> None:
        """停止插件。"""
        pass

    @eventmanager.register(EventType.SubscribeAdded)
    def on_subscribe_added(self, event: MPEvent) -> None:
        """处理订阅添加事件。"""
        if not self._enabled:
            return
        subscribe_id = self._get_subscribe_id(event)
        if not subscribe_id:
            return
        self._activate_new_subscribe(subscribe_id)

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
