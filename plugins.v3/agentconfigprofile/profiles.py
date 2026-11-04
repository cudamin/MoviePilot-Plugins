"""
配置快照、凭据边界、模板保存与系统设置写入。
"""

import hashlib
import re
import threading
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.sdk.logging import logger


class ProfileMixin:
    """
    配置快照、凭据边界、模板保存与系统设置写入。
    """

    def _load_profiles(self) -> List[Dict[str, Any]]:
        data = self.get_data(self.DATA_KEY_PROFILES)
        if isinstance(data, list):
            return data
        return []

    def _save_profiles(self, profiles: List[Dict[str, Any]]):
        with self._lock:
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

    def _capture_snapshot(self) -> Dict[str, Any]:
        """
        捕获当前智能助手配置为快照。
        """
        with self._apply_lock:
            llm: Dict[str, Any] = {}
            current = self._current_llm_config()
            for key in self._LLM_SETTING_KEYS:
                if key in self._SENSITIVE_SETTING_KEYS and not self._include_credentials:
                    continue
                if key in current:
                    llm[key] = current[key]
            return {"snapshot": {"llm": llm}}

    def _current_llm_config(self) -> Dict[str, Any]:
        """
        读取当前生效的 LLM 配置。
        """
        with self._apply_lock:
            return self._real_llm_config()

    @staticmethod
    def _profile_llm(profile: Dict[str, Any]) -> Dict[str, Any]:
        return ((profile or {}).get("snapshot") or {}).get("llm") or {}

    def _effective_llm(self, llm: Dict[str, Any]) -> Dict[str, Any]:
        """
        返回本次实际操作真正会使用的配置；关闭凭据保存时忽略旧模板密钥。
        """
        data = dict(llm or {})
        if not self._include_credentials:
            data.pop("LLM_API_KEY", None)
        return data

    def _sanitize(self, text: Any) -> str:
        """
        清洗错误/日志中的密钥与 Authorization 头，避免二次泄露。
        """
        value = str(text or "")
        value = re.sub(r"(?i)(authorization\s*:\s*bearer\s+)[^\s,;]+", r"\1***", value)
        value = re.sub(r"(?i)([?&](?:api[_-]?key|apikey|token)=)[^&\s]+", r"\1***", value)
        secrets = (
            str(getattr(self._settings, "LLM_API_KEY", "") or ""),
            str(getattr(self._settings, "API_TOKEN", "") or ""),
            str(getattr(self, "_discover_api_key", "") or ""),
            str(getattr(self, "_discovery_api_key_runtime", "") or ""),
        )
        for secret in secrets:
            if len(secret) >= 4:
                value = value.replace(secret, "***")
        return value

    def _update_settings_checked(self, updates: Dict[str, Any], label: str) -> None:
        """
        逐项写入系统设置，失败时尽力回滚本次已成功写入的项目。
        """
        if not updates:
            return
        previous = {key: getattr(self._settings, key, None) for key in updates}
        results = self._settings.update_settings(env=updates)
        failed = [key for key, (ok, _message) in results.items() if ok is False]
        if not failed:
            return
        rollback = {
            key: previous[key]
            for key, (ok, _message) in results.items()
            if ok is True
        }
        if rollback:
            try:
                self._settings.update_settings(env=rollback)
            except Exception as error:  # noqa: BLE001
                logger.error(f"{self.plugin_name}：{label}失败后的回滚异常 - {error}")
        raise RuntimeError(f"{label}失败：{', '.join(failed)}")

    def _apply_llm_config(self, llm: Dict[str, Any]) -> bool:
        """
        将 LLM 配置写回系统设置。
        """
        current = self._current_llm_config()
        env_updates = {}
        for key, value in llm.items():
            if key in self._SENSITIVE_SETTING_KEYS and not self._include_credentials:
                continue
            # 模板未保存密钥时不要把当前可用密钥清空；跨端点复用已在调用方拒绝。
            if key == "LLM_API_KEY" and value in (None, "") and current.get("LLM_API_KEY"):
                continue
            env_updates[key] = value
        if not env_updates:
            return False
        self._update_settings_checked(env_updates, "LLM 配置写入")
        return True

    @staticmethod
    def _identity_text(value: Any) -> str:
        return str(value or "").strip()

    @classmethod
    def _same_endpoint(cls, llm_a: Dict[str, Any], llm_b: Dict[str, Any]) -> bool:
        """
        判断两份配置是否真正等价。

        provider/model/base_url/protocol/preset 必须一致；两边都保存了
        API Key 时还要求 Key 相同，否则同模型不同密钥会被误判为同一份配置。
        只有一边保存 Key 时视为等价，兼容“模板不保存 Key、沿用当前密钥”的用法。
        """
        llm_a = llm_a or {}
        llm_b = llm_b or {}
        for key in ("LLM_PROVIDER", "LLM_MODEL", "LLM_BASE_URL", "LLM_BASE_URL_PRESET"):
            if cls._identity_text(llm_a.get(key)) != cls._identity_text(llm_b.get(key)):
                return False
        # 旧模板可能没有协议字段，按默认 auto 处理，避免升级后全部失效。
        protocol_a = cls._identity_text(llm_a.get("LLM_API_PROTOCOL")) or "auto"
        protocol_b = cls._identity_text(llm_b.get("LLM_API_PROTOCOL")) or "auto"
        if protocol_a != protocol_b:
            return False
        key_a = cls._identity_text(llm_a.get("LLM_API_KEY"))
        key_b = cls._identity_text(llm_b.get("LLM_API_KEY"))
        if key_a and key_b:
            return key_a == key_b
        return True

    @classmethod
    def _fingerprint(cls, llm: Dict[str, Any]) -> str:
        """
        生成用于运行状态绑定的稳定指纹（包含密钥哈希，不落明文）。
        """
        llm = llm or {}
        key = cls._identity_text(llm.get("LLM_API_KEY"))
        key_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16] if key else ""
        parts = [
            cls._identity_text(llm.get("LLM_PROVIDER")),
            cls._identity_text(llm.get("LLM_MODEL")),
            cls._identity_text(llm.get("LLM_BASE_URL")),
            cls._identity_text(llm.get("LLM_API_PROTOCOL")) or "auto",
            cls._identity_text(llm.get("LLM_BASE_URL_PRESET")),
            key_hash,
        ]
        return "|".join(parts)

    @classmethod
    def _health_is_fresh(cls, profile: Dict[str, Any], max_age: Optional[int] = None) -> bool:
        health = (profile or {}).get("health") or {}
        if health.get("status") != "ok":
            return False
        checked_at = str(health.get("checked_at") or "").strip()
        if not checked_at:
            return False
        try:
            checked = datetime.strptime(checked_at, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return False
        age = (datetime.now() - checked).total_seconds()
        return 0 <= age <= (max_age if max_age is not None else cls._HEALTH_TTL_SECONDS)

    @classmethod
    def _profile_key_compatible(cls, llm: Dict[str, Any], current: Dict[str, Any]) -> bool:
        """
        模板没有保存 API Key 时，只有供应商与端点相同才允许复用当前密钥。

        这条规则同时用于探活、应用模板和事件接管，避免把 A 的密钥发到 B 的
        base_url（跨供应商或用户手工改地址时最容易发生）。
        """
        llm = llm or {}
        current = current or {}
        if cls._identity_text(llm.get("LLM_API_KEY")):
            return True
        provider = cls._identity_text(llm.get("LLM_PROVIDER"))
        base_url = cls._identity_text(llm.get("LLM_BASE_URL"))
        if not provider:
            return False
        if provider != cls._identity_text(current.get("LLM_PROVIDER")):
            return False
        if not base_url:
            return True
        return base_url == cls._identity_text(current.get("LLM_BASE_URL"))

    def _profile_is_active(self, profile: Dict[str, Any], current: Dict[str, Any]) -> bool:
        return self._same_endpoint(self._effective_llm(self._profile_llm(profile)), current)

    def _global_llm_values(self) -> Dict[str, Any]:
        """
        返回需要统一到所有模板的 LLM 参数。
        """
        return {
            "LLM_THINKING_LEVEL": self._g_thinking_level,
            "LLM_USE_PROXY": self._g_use_proxy,
            "LLM_API_PROTOCOL": self._g_api_protocol,
            "LLM_WEB_SEARCH_MODE": self._g_web_search_mode,
            "LLM_USER_AGENT": self._g_user_agent,
            "LLM_SUPPORT_IMAGE_INPUT": self._g_image_input,
            "LLM_SUPPORT_AUDIO_INPUT": self._g_audio_input,
            "LLM_SUPPORT_AUDIO_OUTPUT": self._g_audio_output,
        }

    def _global_agent_values(self) -> Dict[str, Any]:
        """
        返回与模型无关、只写入系统设置的全局参数。
        """
        return {
            "AI_AGENT_RETRY_TRANSFER": self._g_retry_transfer,
            "AI_RECOMMEND_ENABLED": self._g_ai_recommend,
            "AI_AGENT_JOB_INTERVAL": self._g_job_interval,
        }

    def _overlay_global(self, llm: Dict[str, Any]) -> Dict[str, Any]:
        """
        把全局参数覆盖到一份 LLM 配置上（未启用时原样返回）。
        """
        if not self._apply_global:
            return llm
        merged = dict(llm or {})
        merged.update(self._global_llm_values())
        return merged

    def _apply_global_options(self, silent: bool = False) -> str:
        """
        把全局参数写入系统设置，并统一覆盖所有模板快照。
        """
        llm_values = self._global_llm_values()
        env_updates = {**llm_values, **self._global_agent_values()}
        if self._mode_active() != "direct":
            gateway = self._mode_state().get("gateway") or {}
            if gateway:
                env_updates.update(self._proxy_projection(gateway, self._mode_active()))

        changed_settings = [key for key, value in env_updates.items()
                            if getattr(self._settings, key, None) != value]
        if changed_settings:
            self._update_settings_checked(env_updates, "全局参数写入")

        touched = 0
        with self._lock:
            profiles = self._load_profiles()
            for profile in profiles:
                llm = self._profile_llm(profile)
                if not llm:
                    continue
                if all(llm.get(key) == value for key, value in llm_values.items()):
                    continue
                llm.update(llm_values)
                profile["updated_at"] = self._now()
                touched += 1
            if touched:
                self._save_profiles(profiles)

        text = f"全局参数已应用：更新 {touched} 个模板，同步 {len(changed_settings)} 项系统设置"
        if not silent:
            self._add_log(text)
            self._save_runtime()
        if touched or changed_settings:
            logger.info(f"{self.plugin_name}：{text}")
        return text

    def _do_save(self, name: str = "") -> str:
        """
        将当前智能助手配置保存为命名模板（同名覆盖）。
        """
        with self._apply_lock:
            snapshot = self._capture_snapshot()
            snapshot["snapshot"]["llm"] = self._overlay_global(snapshot["snapshot"]["llm"])
            llm = snapshot["snapshot"]["llm"]
            if not name:
                name = f"{llm.get('LLM_PROVIDER') or 'llm'} / {llm.get('LLM_MODEL') or 'model'}"
            with self._lock:
                profiles = self._load_profiles()
                existing = self._find_by_name(profiles, name)
                if existing:
                    old_fingerprint = self._fingerprint(
                        self._effective_llm(self._profile_llm(existing)))
                    # 整份替换快照：关闭「模板包含 API Key」后同名更新必须清掉旧密钥，
                    # 不能只更新存在的字段，否则旧 Key 会被永久保留。
                    existing["snapshot"] = snapshot["snapshot"]
                    existing["name"] = name
                    existing["updated_at"] = self._now()
                    if old_fingerprint != self._fingerprint(
                            self._effective_llm(snapshot["snapshot"].get("llm") or {})):
                        existing["health"] = {"status": "unknown", "fail_count": 0}
                else:
                    profiles.append({
                        "id": uuid.uuid4().hex,
                        "name": name,
                        "created_at": self._now(),
                        "updated_at": self._now(),
                        "health": {"status": "unknown", "fail_count": 0},
                        **snapshot,
                    })
                self._save_profiles(profiles)
        logger.info(f"{self.plugin_name}：已保存模板 [{name}]")
        return f"已保存模板 [{name}]"

    def _do_apply(
            self,
            profile_id: str,
            reason: str = "",
            generation: Optional[int] = None,
            stop_event: Optional[threading.Event] = None,
            background: bool = False,
    ) -> str:
        """
        应用指定模板，将其配置写回智能助手并立即生效。
        """
        with self._apply_lock:
            if self._mode_active() != "direct":
                if background and self._mode_active() != "route":
                    return "聚合模式不进行故障转移"
                return self._select_route(profile_id)
            if background and (not getattr(self, "_enabled", False) or self._is_cancelled(generation, stop_event)):
                return "自动切换已停止"
            with self._lock:
                profiles = self._load_profiles()
                profile = self._find_by_id(profiles, profile_id)
                if not profile:
                    raise RuntimeError("应用失败：模板不存在")
                llm = self._effective_llm(self._profile_llm(profile))
                name = profile.get("name")
                if background and (not self._in_failover_queue(profile)
                                   or not self._health_is_fresh(profile)
                                   or self._circuit_for(llm).get_state().value != "closed"):
                    raise RuntimeError("应用失败：配置已移出队列、探活已过期或仍在恢复确认中")
            llm = self._overlay_global(llm)
            if self._apply_global:
                llm = {**llm, **self._global_agent_values()}
            current = self._current_llm_config()
            if not self._profile_key_compatible(llm, current):
                raise RuntimeError(
                    f"应用失败：模板 [{name}] 未包含 API Key，且供应商或端点与当前配置不同，"
                    "已拒绝把当前密钥发往其他端点"
                )
            if not self._apply_llm_config(llm):
                raise RuntimeError(f"应用失败：模板 [{name}] 无可写入配置")
            with self._lock:
                self._runtime["fail_count"] = 0
                self._runtime["fail_endpoint"] = ""
                self._runtime["failover_block_until"] = ""
                self._runtime["takeover_id"] = ""
                self._runtime["applied_at"] = self._now()
                self._add_log(f"已切换到模板 [{name}]{('（' + reason + '）') if reason else ''}")
                self._save_runtime()
            state = self._mode_state()
            state["direct_id"] = profile_id
            state["direct_snapshot"] = dict(llm)
            self._save_mode()
            logger.info(f"{self.plugin_name}：已切换到模板 [{name}] {reason}")
            return f"已切换到模板 [{name}]"

    def _do_delete(self, profile_id: str) -> str:
        with self._lock:
            profiles = self._load_profiles()
            profile = self._find_by_id(profiles, profile_id)
            if not profile:
                return "删除失败：模板不存在"
            if self._mode_active() != "direct" and profile_id == self._mode_state().get("route_id"):
                return "删除失败：请先更换当前路由或聚合默认供应商"
            if self._mode_active() == "stack":
                state = self._mode_state()
                key = next((key for key, pid in state["stack_keys"].items() if pid == profile_id), "")
                if key and str(state.get("selected_model") or "").startswith(f"ccs-{key}/"):
                    return "删除失败：请先选择其他聚合模型"
            profiles = [item for item in profiles if item.get("id") != profile_id]
            self._save_profiles(profiles)
        logger.info(f"{self.plugin_name}：已删除模板 [{profile.get('name')}]")
        return f"已删除模板 [{profile.get('name')}]"

    @staticmethod
    def _in_failover_queue(profile: Dict[str, Any]) -> bool:
        """
        未设置队列标记的旧配置保持参加自动切换。
        """
        return profile.get("in_failover_queue", True) is not False

    def _toggle_queue(self, profile_id: str) -> str:
        """
        保存配置的队列成员状态，不影响手动应用和手动探活。
        """
        with self._apply_lock, self._lock:
            profiles = self._load_profiles()
            profile = self._find_by_id(profiles, profile_id)
            if not profile:
                raise ValueError("配置不存在")
            enabled = not self._in_failover_queue(profile)
            profile["in_failover_queue"] = enabled
            self._save_profiles(profiles)
        return f"已{'加入' if enabled else '移出'}自动切换队列"

    def _do_move(self, profile_id: str, direction: str) -> str:
        """
        调整模板在故障切换顺序中的位置。
        """
        with self._lock:
            profiles = self._load_profiles()
            index = next((i for i, item in enumerate(profiles) if item.get("id") == profile_id), -1)
            if index < 0:
                return "调整失败：模板不存在"
            if direction not in {"up", "down"}:
                raise ValueError("无效的排序方向")
            target = index - 1 if direction == "up" else index + 1
            if target < 0 or target >= len(profiles):
                return "已在边界，无需调整"
            profiles[index], profiles[target] = profiles[target], profiles[index]
            self._save_profiles(profiles)
        return "顺序已调整"

    def _total_pages(self, total: int) -> int:
        """
        按分页大小计算总页数。
        """
        return max(1, (max(0, total) + self._PAGE_SIZE - 1) // self._PAGE_SIZE)

    def _current_page(self, total: int) -> int:
        """
        返回当前有效页码（自动收敛到合法范围）。
        """
        page = self._to_int((self._runtime or {}).get("page"), 1)
        return min(max(1, page), self._total_pages(total))

    def _set_page(self, page: Any) -> str:
        """
        记录模板列表当前页码。
        """
        total = len(self._load_profiles())
        target = min(max(1, self._to_int(page, 1)), self._total_pages(total))
        with self._lock:
            self._runtime["page"] = target
            self._save_runtime()
        return f"已切换到第 {target}/{self._total_pages(total)} 页"
