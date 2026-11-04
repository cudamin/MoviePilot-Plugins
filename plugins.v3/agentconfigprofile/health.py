"""
配置探活、并发任务进度与过期结果处理。
"""

import asyncio
import threading
from contextlib import contextmanager
from typing import Any, Dict, Optional, Tuple

from app.sdk.logging import logger

from .circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitState


class HealthMixin:
    """
    配置探活、并发任务进度与过期结果处理。
    """

    def _circuit_for(self, llm: Dict[str, Any]) -> CircuitBreaker:
        """
        按配置指纹隔离熔断状态，密钥或协议变化后使用独立计数。

        熔断状态只保留在当前插件实例中，不写入配置或数据库。
        """
        identity = dict(llm)
        current = {key: getattr(self._settings, key, None)
                   for key in ("LLM_PROVIDER", "LLM_BASE_URL", "LLM_API_KEY", "LLM_BASE_URL_PRESET")}
        if not identity.get("LLM_API_KEY") and self._profile_key_compatible(identity, current):
            identity["LLM_API_KEY"] = current.get("LLM_API_KEY")
        fingerprint = self._fingerprint(identity)
        with self._lock:
            if not hasattr(self, "_circuits"):
                self._circuits = {}
            circuit = self._circuits.get(fingerprint)
            if circuit is None:
                circuit = CircuitBreaker(CircuitBreakerConfig(failure_threshold=self._fail_threshold))
                self._circuits[fingerprint] = circuit
            return circuit

    @contextmanager
    def _probe_permit(self, llm: Dict[str, Any]):
        """
        为探活取得许可，并在取消、过期或异常退出时归还半开名额。

        :param llm: 本次请求使用的配置快照
        :yields: 熔断器、许可和结果列表；调用方仅在结果仍有效时追加一个布尔值
        """
        circuit = self._circuit_for(llm)
        permit = circuit.allow_request()
        outcome = []
        try:
            yield circuit, permit, outcome
        finally:
            if permit.allowed:
                if outcome:
                    record = circuit.record_success if outcome[0] else circuit.record_failure
                    record(permit.used_half_open_permit)
                else:
                    circuit.release_permit_neutral(permit.used_half_open_permit)

    @staticmethod
    def _run_coro(factory) -> Any:
        """
        在同步上下文中安全执行协程工厂。
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(factory())

        box: Dict[str, Any] = {}

        def _worker():
            try:
                box["value"] = asyncio.run(factory())
            except BaseException as err:  # noqa: BLE001
                box["error"] = err

        thread = threading.Thread(target=_worker, daemon=True)
        thread.start()
        thread.join()
        if "error" in box:
            raise box["error"]
        return box.get("value")

    def _probe(self, llm: Dict[str, Any]) -> Tuple[bool, str, int]:
        """
        对一份 LLM 配置执行一次最小调用探活。

        :return: (是否可用, 描述信息, 耗时毫秒)
        """
        from app.agent.llm.helper import LLMHelper

        provider = (llm or {}).get("LLM_PROVIDER")
        model = (llm or {}).get("LLM_MODEL")
        if not provider or not model:
            return False, "配置缺少供应商或模型", 0
        api_key = str((llm or {}).get("LLM_API_KEY") or "").strip()
        if not api_key:
            # 模板未保存密钥（如关闭了「模板包含 API Key」）时，只有端点与当前配置
            # 一致才允许回落到当前系统密钥；否则拒绝探活，避免把密钥发往其他端点。
            current = self._current_llm_config()
            if self._profile_key_compatible(llm, current):
                api_key = str(current.get("LLM_API_KEY") or "").strip()
            elif self._provider_needs_api_key(provider):
                return False, "未配置 API Key（模板端点与当前配置不一致，已拒绝复用当前密钥）", 0
        if not api_key and self._provider_needs_api_key(provider):
            return False, "未配置 API Key", 0

        kwargs: Dict[str, Any] = {"prompt": self._probe_prompt, "timeout": self._test_timeout}
        for setting_key, arg_name in self._PROBE_ARG_MAP.items():
            if setting_key in llm:
                kwargs[arg_name] = llm.get(setting_key)
        if api_key:
            kwargs["api_key"] = api_key
        if kwargs.get("temperature") is not None:
            try:
                kwargs["temperature"] = float(kwargs["temperature"])
            except (TypeError, ValueError):
                kwargs.pop("temperature", None)

        try:
            result = self._run_coro(lambda: LLMHelper.test_current_settings(**kwargs))
        except Exception as err:  # noqa: BLE001
            message = str(err) or err.__class__.__name__
            if api_key:
                message = message.replace(api_key, "***")
            message = self._sanitize(message)
            # 宿主的 LLMTestError/LLMTestTimeout 会带上已耗时 duration_ms
            return False, message[:180], self._to_int(getattr(err, "duration_ms", 0), 0)

        duration = self._to_int((result or {}).get("duration_ms"), 0)
        if not (result or {}).get("reply_preview"):
            return False, "模型响应为空", duration
        return True, "OK", duration

    def _record_profile_health(self, profile: Dict[str, Any], ok: bool, message: str, duration: int):
        """
        把探活结果写入模板。
        """
        health = profile.setdefault("health", {})
        health["status"] = "ok" if ok else "fail"
        health["checked_at"] = self._now()
        health["message"] = self._sanitize(message)
        health["duration_ms"] = duration
        health["fail_count"] = 0 if ok else self._to_int(health.get("fail_count"), 0) + 1

    def _probe_profile(
            self,
            profile_id: str,
            generation: Optional[int] = None,
            stop_event: Optional[threading.Event] = None,
    ) -> Tuple[bool, str]:
        """
        探活单个模板并持久化结果。
        """
        with self._lock:
            profiles = self._load_profiles()
            profile = self._find_by_id(profiles, profile_id)
            if not profile:
                return False, "模板不存在"
            llm = self._effective_llm(self._profile_llm(profile))
            fingerprint = self._fingerprint(llm)
            name = profile.get("name") or profile_id
        with self._probe_permit(llm) as (_circuit, permit, outcome):
            if not permit.allowed:
                return False, "该配置正在熔断冷却或恢复探测中，已跳过请求"
            if self._is_cancelled(generation, stop_event):
                return False, "探活任务已停止"
            ok, message, duration = self._probe(llm)
            if self._is_cancelled(generation, stop_event):
                return False, "探活任务已停止"
            with self._lock:
                profiles = self._load_profiles()
                profile = self._find_by_id(profiles, profile_id)
                if not profile or self._fingerprint(
                        self._effective_llm(self._profile_llm(profile))) != fingerprint:
                    return False, f"模板 [{name}] 在探活期间已变更或删除，本次结果已忽略"
                self._record_profile_health(profile, ok, message, duration)
                self._save_profiles(profiles)
                outcome.append(ok)
        text = f"模板 [{name}] 探活{'成功' if ok else '失败'}：{message}"
        logger.info(f"{self.plugin_name}：{text}")
        return ok, text

    def _probe_current(
            self,
            generation: Optional[int] = None,
            stop_event: Optional[threading.Event] = None,
    ) -> Tuple[bool, str, int]:
        """
        探活当前生效配置并写入运行状态。
        """
        llm = self._current_llm_config()
        fingerprint = self._fingerprint(llm)
        with self._probe_permit(llm) as (circuit, permit, outcome):
            if not permit.allowed:
                return False, "当前配置正在熔断冷却或恢复探测中，已跳过请求", 0
            if self._is_cancelled(generation, stop_event):
                return False, "探活任务已停止", 0
            ok, message, duration = self._probe(llm)
            if self._is_cancelled(generation, stop_event):
                return False, "探活任务已停止", duration
            latest = self._current_llm_config()
            if self._fingerprint(latest) != fingerprint:
                return False, "当前配置已变更，本次探活结果已忽略", duration
            outcome.append(ok)
        recovered = ok and circuit.get_state() == CircuitState.CLOSED
        with self._lock:
            self._runtime["last_check_at"] = self._now()
            self._runtime["last_check_ok"] = recovered
            self._runtime["last_check_message"] = message
            self._runtime["last_check_duration"] = duration
            if recovered:
                self._runtime["fail_count"] = 0
                self._runtime["fail_endpoint"] = ""
            elif not ok:
                self._runtime["fail_count"] = self._to_int(self._runtime.get("fail_count"), 0) + 1
                self._runtime["fail_endpoint"] = fingerprint
                if circuit.get_state() == CircuitState.OPEN:
                    self._runtime["fail_count"] = max(self._fail_threshold, self._runtime["fail_count"])
            profiles = self._load_profiles()
            changed = False
            for profile in profiles:
                if self._same_endpoint(self._effective_llm(self._profile_llm(profile)), latest):
                    self._record_profile_health(profile, ok, message, duration)
                    changed = True
            if changed:
                self._save_profiles(profiles)
            self._save_runtime()
        if ok and not recovered:
            return False, "恢复探测成功，等待下一次成功确认", duration
        return recovered, message, duration

    def _start_probe_all(self) -> str:
        """
        后台并发探活全部模板，每完成一个立即写入进度，页面刷新即可看到最新结果。
        """
        with self._lock:
            progress = (self._runtime.get("probe_all") or {})
            if progress.get("status") == "running":
                done = self._to_int(progress.get("done"), 0)
                total = self._to_int(progress.get("total"), 0)
                return f"探活正在进行中（{done}/{total}），刷新页面查看进度"

            profiles = self._load_profiles()
            self._runtime["probe_all"] = {
                "status": "running",
                "total": len(profiles) + 1,
                "done": 0,
                "ok": 0,
                "fail": 0,
                "current": "当前生效配置",
                "started_at": self._now(),
                "finished_at": "",
            }
            self._save_runtime()
            generation = self._generation
            stop_event = self._stop_event
        threading.Thread(
            target=self._probe_all_worker,
            args=(generation, stop_event),
            daemon=True,
        ).start()
        return f"已开始探活当前配置与 {len(profiles)} 个模板（{self._PROBE_WORKERS} 并发），刷新页面查看进度"

    def _probe_all_worker(self, generation: int, stop_event: threading.Event):
        """
        探活当前配置与所有模板，多线程并发执行并逐个刷新进度。
        """
        from concurrent.futures import ThreadPoolExecutor

        def bump(ok: bool, current: str):
            if self._is_cancelled(generation, stop_event):
                return
            with self._lock:
                progress = self._runtime.setdefault("probe_all", {})
                progress["done"] = self._to_int(progress.get("done"), 0) + 1
                progress["ok" if ok else "fail"] = self._to_int(progress.get("ok" if ok else "fail"), 0) + 1
                progress["current"] = current
                self._save_runtime()

        try:
            if self._is_cancelled(generation, stop_event):
                return
            ok, _message, _duration = self._probe_current(generation, stop_event)
            bump(ok, "当前生效配置")

            profiles = [p for p in self._load_profiles() if p.get("id")]
            if profiles and not self._is_cancelled(generation, stop_event):
                def probe_one(profile: Dict[str, Any]):
                    if self._is_cancelled(generation, stop_event):
                        return
                    name = profile.get("name") or ""
                    result, _text = self._probe_profile(profile.get("id"), generation, stop_event)
                    bump(result, name)

                workers = max(1, min(self._PROBE_WORKERS, len(profiles)))
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    list(executor.map(probe_one, profiles))

            if self._is_cancelled(generation, stop_event):
                return
            with self._lock:
                progress = self._runtime.setdefault("probe_all", {})
                progress["status"] = "done"
                progress["current"] = ""
                progress["finished_at"] = self._now()
                self._add_log(f"全量探活完成：正常 {progress.get('ok', 0)} 个，失败 {progress.get('fail', 0)} 个")
                self._save_runtime()
                logger.info(f"{self.plugin_name}：全量探活完成 正常 {progress.get('ok')} 失败 {progress.get('fail')}")
        except Exception as err:  # noqa: BLE001
            with self._lock:
                progress = self._runtime.setdefault("probe_all", {})
                progress["status"] = "error"
                progress["message"] = self._sanitize(err)[:180]
                progress["finished_at"] = self._now()
                self._save_runtime()
            logger.error(f"{self.plugin_name}：全量探活失败 - {err}")
        finally:
            # 取消/停用时必须把 running 收敛到终态；否则页面会一直显示进行中，
            # 直到下一次插件初始化才被 _mark_interrupted_states 修正。
            if generation == getattr(self, "_generation", generation) \
                    and self._is_cancelled(generation, stop_event):
                with self._lock:
                    progress = self._runtime.setdefault("probe_all", {})
                    if progress.get("status") == "running":
                        progress["status"] = "error"
                        progress["message"] = "任务已停止"
                        progress["current"] = ""
                        progress["finished_at"] = self._now()
                        self._save_runtime()
