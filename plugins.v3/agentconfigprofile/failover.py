"""
故障转移、首选配置恢复与切换通知。
"""

import threading
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from app.schemas.types import MessageType
from app.sdk.logging import logger

from .circuit_breaker import CircuitState


class FailoverMixin:
    """
    故障转移、首选配置恢复与切换通知。
    """

    def _healthy_backup(self, current: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        按顺序返回第一个健康状态新鲜、密钥兼容且不是当前配置的模板。
        """
        for profile in self._load_profiles():
            if not self._in_failover_queue(profile):
                continue
            llm = self._effective_llm(self._profile_llm(profile))
            if self._same_endpoint(llm, current):
                continue
            if self._circuit_for(llm).get_state() != CircuitState.CLOSED:
                continue
            if not self._health_is_fresh(profile):
                continue
            if not self._profile_key_compatible(llm, current):
                continue
            return profile
        return None

    def _notify_safe(self, title: str, text: str) -> None:
        """
        通知失败不得影响已经完成的切换或任务状态。
        """
        if not self._notify:
            return
        try:
            self.post_message(mtype=MessageType.Plugin, title=title, text=self._sanitize(text))
        except Exception as err:  # noqa: BLE001
            logger.warning(f"{self.plugin_name}：发送通知失败 - {err}")

    def _do_failover(
            self,
            reason: str,
            generation: Optional[int] = None,
            stop_event: Optional[threading.Event] = None,
    ) -> str:
        """
        串行执行故障切换，避免多个入口同时改写系统 LLM 配置。
        """
        if self._mode_active() == "stack":
            return "聚合模式不进行故障转移"
        if self._mode_active() == "direct" and self._mode_state().get("explicit_mode"):
            return "直连模式不进行代理故障转移"
        if not getattr(self, "_enabled", False) or self._is_cancelled(generation, stop_event):
            return "自动切换已停止"
        with self._lock:
            block_until = str(self._runtime.get("failover_block_until") or "")
            if block_until:
                try:
                    if datetime.now() < datetime.strptime(block_until, "%Y-%m-%d %H:%M:%S"):
                        return "自动切换失败后处于冷却期，稍后再试"
                except ValueError:
                    pass
        if not self._failover_lock.acquire(blocking=False):
            return "自动切换正在进行中"
        try:
            return self._do_failover_locked(reason, generation, stop_event)
        finally:
            self._failover_lock.release()

    def _do_failover_locked(
            self,
            reason: str,
            generation: Optional[int] = None,
            stop_event: Optional[threading.Event] = None,
    ) -> str:
        """
        按模板顺序寻找可用配置并切换。
        """
        if self._mode_active() == "stack":
            return "聚合模式不进行故障转移"
        if self._mode_active() == "direct" and self._mode_state().get("explicit_mode"):
            return "直连模式不进行代理故障转移"
        if not getattr(self, "_enabled", False) or self._is_cancelled(generation, stop_event):
            return "自动切换已停止"
        current = self._current_llm_config()
        profiles = self._load_profiles()
        candidates = [
            p for p in profiles
            if self._in_failover_queue(p)
            and self._circuit_for(self._effective_llm(self._profile_llm(p))).is_available()
            and not self._same_endpoint(self._effective_llm(self._profile_llm(p)), current)
            and self._profile_key_compatible(self._effective_llm(self._profile_llm(p)), current)
        ]
        if not candidates:
            text = "自动切换失败：没有可用的备用模板"
            with self._lock:
                if self._FAILOVER_COOLDOWN:
                    self._runtime["failover_block_until"] = (
                        datetime.now() + timedelta(seconds=self._FAILOVER_COOLDOWN)
                    ).strftime("%Y-%m-%d %H:%M:%S")
                self._add_log(text, "error")
                self._save_runtime()
            logger.warning(f"{self.plugin_name}：{text}")
            return text

        # 与 CC Switch provider_router 相同，严格使用用户设置的队列顺序。
        tried = 0
        for profile in candidates:
            if self._is_cancelled(generation, stop_event):
                return "自动切换已停止"
            if self._FAILOVER_MAX_TRY and tried >= self._FAILOVER_MAX_TRY:
                break
            tried += 1
            try:
                ok, _text = self._probe_profile(profile.get("id"), generation, stop_event)
                if not ok:
                    continue
                if self._circuit_for(self._effective_llm(self._profile_llm(profile))).get_state() != CircuitState.CLOSED:
                    continue
                message = self._do_apply(
                    profile.get("id"), reason=reason, generation=generation,
                    stop_event=stop_event, background=True,
                )
                if message.startswith("自动切换已停止") or message.startswith("应用失败"):
                    return message
            except Exception as err:  # noqa: BLE001
                logger.warning(f"{self.plugin_name}：模板 [{profile.get('name')}] 切换失败 - {err}")
                continue
            with self._lock:
                self._runtime["failover_block_until"] = ""
                self._save_runtime()
            self._notify_safe(
                "智能助手已自动切换配置",
                f"原因：{reason}\n"
                f"新配置：{self._profile_llm(profile).get('LLM_PROVIDER')} / "
                f"{self._profile_llm(profile).get('LLM_MODEL')}\n"
                f"模板：{profile.get('name')}",
            )
            return message

        text = f"自动切换失败：已尝试 {tried} 个备用模板，探活或写入均失败"
        with self._lock:
            if self._FAILOVER_COOLDOWN:
                self._runtime["failover_block_until"] = (
                    datetime.now() + timedelta(seconds=self._FAILOVER_COOLDOWN)
                ).strftime("%Y-%m-%d %H:%M:%S")
            self._add_log(text, "error")
            self._save_runtime()
        logger.warning(f"{self.plugin_name}：{text}")
        self._notify_safe(
            "智能助手配置自动切换失败",
            f"原因：{reason}\n{text}，请检查网络或密钥。",
        )
        return text

    def _try_recover_primary(
            self,
            current: Dict[str, Any],
            generation: Optional[int] = None,
            stop_event: Optional[threading.Event] = None,
    ) -> Optional[str]:
        """
        当前配置正常但不是首选模板时，尝试切回首选模板。
        """
        if not getattr(self, "_enabled", False) or self._is_cancelled(generation, stop_event):
            return None
        profiles = [p for p in self._load_profiles() if self._in_failover_queue(p)]
        if not profiles:
            return None
        primary = profiles[0]
        primary_llm = self._effective_llm(self._profile_llm(primary))
        if self._same_endpoint(primary_llm, current):
            return None
        if not self._profile_key_compatible(primary_llm, current):
            return None
        try:
            circuit = self._circuit_for(primary_llm)
            if not circuit.is_available():
                return None
            if not self._health_is_fresh(primary) or circuit.get_state() != CircuitState.CLOSED:
                ok, _text = self._probe_profile(primary.get("id"), generation, stop_event)
                if not ok or circuit.get_state() != CircuitState.CLOSED:
                    return None
            message = self._do_apply(
                primary.get("id"), reason="首选模板已恢复", generation=generation,
                stop_event=stop_event, background=True,
            )
        except Exception as err:  # noqa: BLE001
            logger.warning(f"{self.plugin_name}：切回首选模板失败 - {err}")
            return None
        self._notify_safe("智能助手已切回首选配置", f"模板：{primary.get('name')}")
        return message

    def health_check_job(self) -> str:
        """
        定时任务：探活当前配置，失效达阈值后自动切换。
        """
        if not self._enabled:
            return "插件未启用"
        if self._mode_active() == "stack":
            return "聚合模式由选定模型固定供应商，不进行故障转移"
        generation = self._generation
        stop_event = self._stop_event
        ok, message, duration = self._probe_current(generation, stop_event)
        if self._is_cancelled(generation, stop_event) or not self._enabled:
            return "自动切换已停止"
        if message.startswith("当前配置已变更"):
            return message
        if ok:
            logger.info(f"{self.plugin_name}：当前配置探活正常（{duration}ms）")
            if self._auto_recover:
                try:
                    recovered = self._try_recover_primary(
                        self._current_llm_config(), generation, stop_event)
                except Exception as err:  # noqa: BLE001
                    logger.warning(f"{self.plugin_name}：自动恢复首选模板失败 - {err}")
                    recovered = None
                if recovered:
                    return recovered
            return f"当前配置正常（{duration}ms）"

        fail_count = self._to_int(self._runtime.get("fail_count"), 0)
        text = f"当前配置探活失败（第 {fail_count} 次）：{message}"
        with self._lock:
            self._add_log(text, "warn")
            self._save_runtime()
        logger.warning(f"{self.plugin_name}：{text}")

        if not self._auto_failover:
            return text
        if fail_count < self._fail_threshold:
            return text
        return self._do_failover(
            reason=f"当前模型连续 {fail_count} 次探活失败：{message}",
            generation=generation,
            stop_event=stop_event,
        )

    def _failover_in_background(
            self,
            reason: str,
            generation: Optional[int] = None,
            stop_event: Optional[threading.Event] = None,
    ):
        """
        后台执行一次故障切换，避免阻塞事件链。
        """
        try:
            if self._is_cancelled(generation, stop_event):
                return
            self._do_failover(reason=reason, generation=generation, stop_event=stop_event)
        except Exception as err:  # noqa: BLE001
            logger.error(f"{self.plugin_name}：后台自动切换失败 - {err}")
        finally:
            if generation is None or generation == getattr(self, "_generation", generation):
                with self._lock:
                    self._takeover_pending = False
