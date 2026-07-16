import threading
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from app import schemas
from app.core.config import settings
from app.log import logger
from app.plugins import _PluginBase

class AgentConfigProfile(_PluginBase):
    """
    智能助手配置模板插件。

    将当前智能助手（Agent）的 LLM 供应商配置保存为命名模板，
    支持保存多个模板并一键快速切换，方便在不同场景之间来回切换。
    采用 Vuetify（免构建）渲染模式。
    """

    plugin_name = "智能助手配置模板"
    plugin_desc = "持久化保存智能助手的 LLM 配置，支持多个模板一键快速切换。"
    plugin_icon = "agentresourceofficer.png"
    plugin_version = "1.1.1"
    plugin_author = "tafei"
    author_url = "https://github.com/cudamin/MoviePilot-Plugins"
    plugin_config_prefix = "agentconfigprofile_"
    plugin_order = 46
    auth_level = 1

    _LLM_SETTING_KEYS = [
        "LLM_PROVIDER",
        "LLM_MODEL",
        "LLM_API_KEY",
        "LLM_BASE_URL",
        "LLM_BASE_URL_PRESET",
        "LLM_USER_AGENT",
        "LLM_USE_PROXY",
        "LLM_THINKING_LEVEL",
        "LLM_MAX_CONTEXT_TOKENS",
        "LLM_TEMPERATURE",
        "LLM_SUPPORT_IMAGE_INPUT",
        "LLM_SUPPORT_AUDIO_INPUT",
        "LLM_SUPPORT_AUDIO_OUTPUT",
    ]

    _SENSITIVE_SETTING_KEYS = {"LLM_API_KEY"}

    DATA_KEY_PROFILES = "profiles"

    def init_plugin(self, config: dict = None):
        """初始化插件配置，并处理来自配置页的一次性动作。"""
        self._lock = threading.RLock()
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._include_credentials = bool(config.get("include_credentials", True))

        action = str(config.get("action") or "none").strip()
        new_profile_name = str(config.get("new_profile_name") or "").strip()
        selected_profile = str(config.get("selected_profile") or "").strip()
        action_message = ""

        try:
            if action == "save":
                action_message = self._do_save(new_profile_name)
            elif action == "apply":
                action_message = self._do_apply(selected_profile)
            elif action == "delete":
                action_message = self._do_delete(selected_profile)
        except Exception as err:
            action_message = f"操作失败：{err}"
            logger.error(f"智能助手配置模板：{action_message}")

        self._action_message = action_message
        self._save_persistent_config(action_message=action_message)

    def _save_persistent_config(self, action_message: str = ""):
        """持久化配置并清空一次性动作字段，避免重复执行。"""
        self.update_config({
            "enabled": self._enabled,
            "include_credentials": self._include_credentials,
            "action": "none",
            "new_profile_name": "",
            "selected_profile": "",
            "last_action_message": action_message,
            "last_action_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S") if action_message else "",
        })

    def get_state(self) -> bool:
        return bool(getattr(self, "_enabled", False))

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def stop_service(self):
        pass

    # ------------------------------------------------------------------
    # 动作实现
    # ------------------------------------------------------------------

    def _do_save(self, name: str) -> str:
        """将当前智能助手配置保存为命名模板（同名则覆盖）。"""
        if not name:
            return "保存失败：模板名称不能为空"
        with self._lock:
            profiles = self._load_profiles()
            snapshot = self._capture_snapshot()
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            existing = self._find_by_name(profiles, name)
            if existing:
                existing.update(snapshot)
                existing["name"] = name
                existing["updated_at"] = now
            else:
                profiles.append({
                    "id": uuid.uuid4().hex,
                    "name": name,
                    "created_at": now,
                    "updated_at": now,
                    **snapshot,
                })
            self._save_profiles(profiles)
        logger.info(f"智能助手配置模板：已保存模板 [{name}]")
        return f"已保存模板 [{name}]"

    def _do_apply(self, profile_id: str) -> str:
        """应用指定模板，将其配置写回智能助手并立即生效。"""
        if not profile_id:
            return "应用失败：未选择模板"
        with self._lock:
            profiles = self._load_profiles()
            profile = self._find_by_id(profiles, profile_id)
            if not profile:
                return "应用失败：模板不存在"
            applied = self._apply_snapshot(profile)
        logger.info(f"智能助手配置模板：已切换到模板 [{profile.get('name')}]（{applied}）")
        return f"已切换到模板 [{profile.get('name')}]：{applied}"

    def _do_delete(self, profile_id: str) -> str:
        """删除指定模板。"""
        if not profile_id:
            return "删除失败：未选择模板"
        with self._lock:
            profiles = self._load_profiles()
            profile = self._find_by_id(profiles, profile_id)
            if not profile:
                return "删除失败：模板不存在"
            profiles = [item for item in profiles if item.get("id") != profile_id]
            self._save_profiles(profiles)
        logger.info(f"智能助手配置模板：已删除模板 [{profile.get('name')}]")
        return f"已删除模板 [{profile.get('name')}]"

    def _capture_snapshot(self) -> Dict[str, Any]:
        """捕获当前智能助手配置为快照。"""
        snapshot: Dict[str, Any] = {}

        llm: Dict[str, Any] = {}
        for key in self._LLM_SETTING_KEYS:
            if key in self._SENSITIVE_SETTING_KEYS and not self._include_credentials:
                continue
            if hasattr(settings, key):
                llm[key] = getattr(settings, key)
        snapshot["llm"] = llm

        return {"snapshot": snapshot}

    def _apply_snapshot(self, profile: Dict[str, Any]) -> str:
        """将模板快照写回智能助手配置并立即生效，返回已应用项描述。"""
        snapshot = profile.get("snapshot") or {}
        applied: List[str] = []

        llm = snapshot.get("llm") or {}
        env_updates = {}
        for key, value in llm.items():
            if key in self._SENSITIVE_SETTING_KEYS and not self._include_credentials:
                continue
            env_updates[key] = value
        if env_updates:
            results = settings.update_settings(env=env_updates)
            failed = [k for k, (ok, _msg) in results.items() if ok is False]
            if failed:
                logger.warning(f"智能助手配置模板：部分 LLM 配置写入失败 - {failed}")
            applied.append("LLM配置")

        return "、".join(applied) if applied else "无变更"

    # ------------------------------------------------------------------
    # 数据存取
    # ------------------------------------------------------------------

    def _load_profiles(self) -> List[Dict[str, Any]]:
        """读取已保存的模板列表。"""
        data = self.get_data(self.DATA_KEY_PROFILES)
        if isinstance(data, list):
            return data
        return []

    def _save_profiles(self, profiles: List[Dict[str, Any]]):
        """持久化模板列表。"""
        self.save_data(self.DATA_KEY_PROFILES, profiles)

    @staticmethod
    def _find_by_id(profiles: List[Dict[str, Any]], profile_id: str) -> Optional[Dict[str, Any]]:
        for item in profiles:
            if item.get("id") == profile_id:
                return item
        return None

    @staticmethod
    def _find_by_name(profiles: List[Dict[str, Any]], name: str) -> Optional[Dict[str, Any]]:
        for item in profiles:
            if item.get("name") == name:
                return item
        return None

    def _current_summary(self) -> Dict[str, Any]:
        """返回当前生效配置的摘要。"""
        summary = {
            "provider": getattr(settings, "LLM_PROVIDER", None),
            "model": getattr(settings, "LLM_MODEL", None),
            "base_url": getattr(settings, "LLM_BASE_URL", None),
            "has_api_key": bool(getattr(settings, "LLM_API_KEY", None)),
        }
        return summary

    def _profile_is_active(self, profile: Dict[str, Any], current: Dict[str, Any]) -> bool:
        """判断某模板是否与当前生效配置一致。"""
        snapshot = profile.get("snapshot") or {}
        llm = snapshot.get("llm") or {}
        return (
            llm.get("LLM_PROVIDER") == current.get("provider")
            and llm.get("LLM_MODEL") == current.get("model")
            and llm.get("LLM_BASE_URL") == current.get("base_url")
        )

    # ------------------------------------------------------------------
    # 配置页（Vuetify）
    # ------------------------------------------------------------------

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """拼装配置页：范围开关 + 保存/应用/删除动作。"""
        profiles = self._load_profiles()
        profile_items = [
            {"title": f"{p.get('name')}（{(p.get('snapshot') or {}).get('llm', {}).get('LLM_MODEL') or '-'}）",
             "value": p.get("id")}
            for p in profiles
        ]
        action_items = [
            {"title": "（不执行）", "value": "none"},
            {"title": "保存当前配置为新模板", "value": "save"},
            {"title": "应用选中的模板", "value": "apply"},
            {"title": "删除选中的模板", "value": "delete"},
        ]
        last_msg = self.get_config().get("last_action_message") if self.get_config() else ""

        form = [
            {
                "component": "VAlert",
                "props": {
                    "type": "info", "variant": "tonal", "class": "mb-4",
                    "text": "把当前智能助手的 LLM 配置保存为模板，随时一键切换。"
                            "先在下方选择动作并保存插件即可执行；执行结果见页面顶部提示与“数据详情”页。"
                },
            },
        ]
        if last_msg:
            form.append({
                "component": "VAlert",
                "props": {"type": "success", "variant": "tonal", "class": "mb-4",
                          "text": f"上次操作：{last_msg}"},
            })

        form.extend([
            {
                "component": "VRow",
                "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 6},
                     "content": [{"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]},
                ],
            },
            {
                "component": "VRow",
                "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 3},
                     "content": [{"component": "VSwitch", "props": {"model": "include_credentials", "label": "含凭据(API Key)"}}]},
                ],
            },
            {"component": "VDivider", "props": {"class": "my-3"}},
            {
                "component": "VRow",
                "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VSelect", "props": {
                         "model": "action", "label": "执行动作", "items": action_items}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VSelect", "props": {
                         "model": "selected_profile", "label": "选择模板（应用/删除）",
                         "items": profile_items, "clearable": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VTextField", "props": {
                         "model": "new_profile_name", "label": "新模板名称（保存时填写）"}}]},
                ],
            },
            {
                "component": "VAlert",
                "props": {"type": "warning", "variant": "tonal", "class": "mt-2", "density": "compact",
                          "text": "含凭据的模板会以明文保存 API Key，请谨慎处理。切换后新会话立即生效。"},
            },
        ])

        default_config = {
            "enabled": False,
            "include_credentials": True,
            "action": "none",
            "selected_profile": "",
            "new_profile_name": "",
        }
        return form, default_config

    # ------------------------------------------------------------------
    # 详情页（Vuetify）
    # ------------------------------------------------------------------

    def get_page(self) -> List[dict]:
        """展示当前生效配置与已保存模板列表。"""
        current = self._current_summary()
        profiles = self._load_profiles()

        header_chips = [
            {"component": "VChip", "props": {"color": "primary", "variant": "flat", "class": "me-2"},
             "text": f"供应商 {current.get('provider') or '-'}"},
            {"component": "VChip", "props": {"color": "primary", "variant": "flat", "class": "me-2"},
             "text": f"模型 {current.get('model') or '-'}"},
            {"component": "VChip",
             "props": {"color": "success" if current.get("has_api_key") else "warning", "variant": "flat"},
             "text": "API Key 已配置" if current.get("has_api_key") else "API Key 未配置"},
        ]

        table_rows = []
        for p in profiles:
            snapshot = p.get("snapshot") or {}
            llm = snapshot.get("llm") or {}
            active = self._profile_is_active(p, current)
            table_rows.append({
                "component": "tr",
                "content": [
                    {"component": "td", "props": {"class": "text-high-emphasis font-weight-medium"},
                     "content": [
                         {"component": "span", "text": p.get("name") or "-"},
                         {"component": "VChip",
                          "props": {"size": "x-small", "color": "success", "variant": "flat", "class": "ms-2"},
                          "text": "生效中"} if active else {"component": "span", "text": ""},
                     ]},
                    {"component": "td", "text": llm.get("LLM_PROVIDER") or "-"},
                    {"component": "td", "text": llm.get("LLM_MODEL") or "-"},
                    {"component": "td", "text": "是" if llm.get("LLM_API_KEY") else "否"},
                    {"component": "td", "text": p.get("updated_at") or "-"},
                ],
            })

        if not table_rows:
            body = [{"component": "VAlert", "props": {
                "type": "info", "variant": "tonal",
                "text": "还没有保存任何模板。前往插件配置页，选择“保存当前配置为新模板”并填写名称后保存插件即可创建。"}}]
        else:
            body = [{
                "component": "VTable",
                "props": {"hover": True},
                "content": [
                    {"component": "thead", "content": [{"component": "tr", "content": [
                        {"component": "th", "text": "模板名称"},
                        {"component": "th", "text": "供应商"},
                        {"component": "th", "text": "模型"},
                        {"component": "th", "text": "含密钥"},
                        {"component": "th", "text": "更新时间"},
                    ]}]},
                    {"component": "tbody", "content": table_rows},
                ],
            }]

        return [
            {
                "component": "VCard",
                "props": {"variant": "tonal", "class": "mb-4"},
                "content": [
                    {"component": "VCardTitle", "props": {"class": "text-subtitle-1"}, "text": "当前生效配置"},
                    {"component": "VCardText",
                     "content": [{"component": "div", "props": {"class": "d-flex flex-wrap align-center"},
                                  "content": header_chips}]},
                ],
            },
            {
                "component": "VCard",
                "content": [
                    {"component": "VCardTitle", "props": {"class": "text-subtitle-1"},
                     "text": f"已保存模板（{len(profiles)}）"},
                    {"component": "VCardText", "content": body},
                ],
            },
        ]
