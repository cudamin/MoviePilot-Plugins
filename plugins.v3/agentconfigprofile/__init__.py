import asyncio
import hashlib
import re
import threading
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.triggers.cron import CronTrigger

from app import schemas
from app.plugins import _PluginBase
from app.schemas.types import ChainEventType, NotificationType
from app.sdk.config import settings
from app.sdk.events import Event, eventmanager
from app.sdk.logging import logger


class agentconfigprofile(_PluginBase):
    """
    API 聚合自动切换插件。

    将当前智能助手（Agent）的 LLM 供应商配置保存为命名模板，支持一键切换；
    并可定时探活当前生效配置，失效时按模板顺序自动切换到可用配置。
    采用 Vuetify（免构建）渲染模式，详情页即操作台。

    注意：插件 ID 恒等于类名，这里刻意使用全小写类名，与插件市场安装统计
    服务端已有的 `agentconfigprofile` 记录保持一致，否则市场卡片读不到下载量。
    旧版大写 ID（AgentConfigProfile）的配置与数据会在首次加载时自动迁移。
    """

    plugin_name = "API聚合自动切换"
    plugin_desc = "保存智能助手 LLM 配置模板，一键切换，探测端点可用模型自动建模板，并在模型失效时自动切换。"
    plugin_icon = "agentresourceofficer.png"
    plugin_version = "2.8.0"
    plugin_author = "tafei"
    author_url = "https://github.com/cudamin"
    # 插件市场仓库地址，安装统计上报时一并提交
    plugin_repo_url = "https://github.com/cudamin/MoviePilot-Plugins"
    # 旧版插件 ID（大写类名），用于配置与数据迁移
    _LEGACY_PLUGIN_ID = "AgentConfigProfile"
    plugin_config_prefix = "agentconfigprofile_"
    plugin_order = 46
    auth_level = 1

    # 纳入模板快照的 LLM 配置项
    _LLM_SETTING_KEYS = [
        "LLM_PROVIDER",
        "LLM_MODEL",
        "LLM_API_KEY",
        "LLM_BASE_URL",
        "LLM_BASE_URL_PRESET",
        "LLM_USER_AGENT",
        "LLM_USE_PROXY",
        "LLM_THINKING_LEVEL",
        "LLM_API_PROTOCOL",
        "LLM_WEB_SEARCH_MODE",
        "LLM_MAX_CONTEXT_TOKENS",
        "LLM_TEMPERATURE",
        "LLM_SUPPORT_IMAGE_INPUT",
        "LLM_SUPPORT_AUDIO_INPUT",
        "LLM_SUPPORT_AUDIO_OUTPUT",
    ]

    _SENSITIVE_SETTING_KEYS = {"LLM_API_KEY"}

    # 思考模式可选项（值, 展示名），与系统设置页保持一致
    _THINKING_LEVEL_OPTIONS = (
        ("off", "关闭"), ("auto", "自动"), ("minimal", "最低"), ("low", "低"),
        ("medium", "中"), ("high", "高"), ("xhigh", "极高"), ("max", "最大"),
    )
    # OpenAI 兼容接口请求协议可选项
    _API_PROTOCOL_OPTIONS = (
        ("auto", "自动"),
        ("chat_completions", "Chat Completions"),
        ("responses", "Responses"),
    )
    # 联网搜索模式可选项
    _WEB_SEARCH_MODE_OPTIONS = (
        ("local", "MoviePilot 本地搜索"),
        ("builtin", "模型服务商检索"),
        ("auto", "自动回退"),
        ("disabled", "完全关闭"),
    )
    # 定时唤醒间隔可选项（小时），0 为不启用
    _JOB_INTERVAL_OPTIONS = (1, 2, 4, 6, 12, 24, 48)

    # 探活时可直接传给 LLMHelper.test_current_settings 的参数映射
    _PROBE_ARG_MAP = {
        "LLM_PROVIDER": "provider",
        "LLM_MODEL": "model",
        "LLM_API_KEY": "api_key",
        "LLM_BASE_URL": "base_url",
        "LLM_BASE_URL_PRESET": "base_url_preset",
        "LLM_USER_AGENT": "user_agent",
        "LLM_USE_PROXY": "use_proxy",
        "LLM_THINKING_LEVEL": "thinking_level",
        "LLM_API_PROTOCOL": "api_protocol",
        "LLM_WEB_SEARCH_MODE": "web_search_mode",
        "LLM_TEMPERATURE": "temperature",
    }

    DATA_KEY_PROFILES = "profiles"
    DATA_KEY_RUNTIME = "runtime"
    DATA_KEY_DISCOVERY = "discovery"

    _LOG_MAX = 30
    _DEFAULT_CRON = "*/30 * * * *"
    # 模板列表分页大小
    _PAGE_SIZE = 15
    # 全量探活并发数
    _PROBE_WORKERS = 4
    # 一次故障切换最多真实探活的候选模板数
    _FAILOVER_MAX_TRY = 8
    # 安装统计上报失败后的重试间隔（秒）
    _REPORT_RETRY_INTERVAL = 3600
    # 模板健康状态的有效期（秒）；超过后不再用于事件接管
    _HEALTH_TTL_SECONDS = 1800
    # 一轮自动切换失败后的冷却时间（秒），避免每个探活周期重复试错
    _FAILOVER_COOLDOWN = 60
    # 旧版插件 ID 迁移完成标记，写在旧配置里，避免重置新插件后再次复活旧数据
    _LEGACY_MIGRATED_KEY = "_migrated_to_agentconfigprofile"

    # 支持探测的协议：provider id -> 展示名
    _DISCOVER_PROVIDERS = {
        "openai": "OpenAI 兼容",
        "anthropic": "Anthropic",
        "deepseek": "DeepSeek",
    }

    # 智能匹配：模型名关键字 -> 优先协议
    _MODEL_PROVIDER_HINTS = (
        ("anthropic", ("claude", "sonnet", "opus", "haiku")),
        ("deepseek", ("deepseek",)),
        ("openai", ("gpt", "chatgpt", "codex", "o1-", "o3-", "o4-")),
    )
    _PROVIDER_FALLBACK_ORDER = ("openai", "anthropic", "deepseek")

    def __init__(self) -> None:
        super().__init__()
        self._lock = threading.RLock()
        self._failover_lock = threading.Lock()
        self._apply_lock = threading.RLock()
        self._stop_event = threading.Event()
        self._generation = 0
        self._runtime: Dict[str, Any] = {}
        self._takeover_pending = False

    def init_plugin(self, config: dict = None):
        """初始化插件配置，并处理来自配置页的一次性动作。"""
        # v3 initialize() 会在同一个实例上再次调用 init_plugin()。先停止旧代后台
        # 任务，并保留同一把锁，避免旧线程与新配置各自使用不同锁而发生交叉写入。
        previous_stop_event = getattr(self, "_stop_event", None)
        if previous_stop_event is not None:
            previous_stop_event.set()
        if not isinstance(getattr(self, "_lock", None), type(threading.RLock())):
            self._lock = threading.RLock()
        if not hasattr(self, "_failover_lock"):
            self._failover_lock = threading.Lock()
        if not isinstance(getattr(self, "_apply_lock", None), type(threading.RLock())):
            self._apply_lock = threading.RLock()
        self._generation = getattr(self, "_generation", 0) + 1
        self._stop_event = threading.Event()
        config = self._migrate_legacy_id(config or {})
        self._active_tab = str(config.get("_active_tab") or "basic")
        self._enabled = bool(config.get("enabled"))
        self._include_credentials = bool(config.get("include_credentials", True))
        self._notify = bool(config.get("notify", True))
        self._auto_failover = bool(config.get("auto_failover"))
        self._auto_recover = bool(config.get("auto_recover"))
        self._takeover = bool(config.get("takeover", True))
        self._check_cron = self._valid_cron(config.get("check_cron"))
        self._fail_threshold = max(1, self._to_int(config.get("fail_threshold"), 2))
        self._test_timeout = max(5, self._to_int(config.get("test_timeout"), 20))
        self._probe_prompt = str(config.get("probe_prompt") or "请只回复 OK").strip() or "请只回复 OK"

        # 模型探测配置
        self._discover_base_url = str(config.get("discover_base_url") or "").strip()
        self._discover_api_key = str(config.get("discover_api_key") or "").strip()
        providers = config.get("discover_providers")
        if isinstance(providers, str):
            providers = [p.strip() for p in providers.split(",") if p.strip()]
        self._discover_providers = [p for p in (providers or list(self._DISCOVER_PROVIDERS))
                                    if p in self._DISCOVER_PROVIDERS] or list(self._DISCOVER_PROVIDERS)
        self._discover_filter = str(config.get("discover_filter") or "").strip()
        self._discover_limit = max(0, self._to_int(config.get("discover_limit"), 20))
        self._discover_auto_import = bool(config.get("discover_auto_import", True))
        self._discover_smart_match = bool(config.get("discover_smart_match", True))
        # 旧版按协议拆分的三个白名单输入框（whitelist_<provider>）合并为单一白名单
        if not str(config.get("whitelist") or "").strip():
            merged = []
            for key in ("whitelist_anthropic", "whitelist_openai", "whitelist_deepseek"):
                merged.extend(self._parse_list(config.get(key)))
            if merged:
                config["whitelist"] = ", ".join(merged)
                logger.info(f"{self.plugin_name}：已合并旧版按协议拆分的模型白名单，共 {len(merged)} 项")
        self._whitelist = self._parse_list(config.get("whitelist"))

        # 全局参数（统一应用到所有模板与系统设置）
        self._apply_global = bool(config.get("apply_global"))
        self._g_thinking_level = str(
            config.get("g_thinking_level") or getattr(settings, "LLM_THINKING_LEVEL", "off") or "off")
        self._g_use_proxy = bool(config.get("g_use_proxy", getattr(settings, "LLM_USE_PROXY", True)))
        self._g_image_input = bool(config.get("g_image_input", getattr(settings, "LLM_SUPPORT_IMAGE_INPUT", True)))
        self._g_audio_input = bool(config.get("g_audio_input", getattr(settings, "LLM_SUPPORT_AUDIO_INPUT", False)))
        self._g_audio_output = bool(config.get("g_audio_output", getattr(settings, "LLM_SUPPORT_AUDIO_OUTPUT", False)))
        self._g_retry_transfer = bool(
            config.get("g_retry_transfer", getattr(settings, "AI_AGENT_RETRY_TRANSFER", False)))
        self._g_ai_recommend = bool(config.get("g_ai_recommend", getattr(settings, "AI_RECOMMEND_ENABLED", False)))
        self._g_api_protocol = str(
            config.get("g_api_protocol") or getattr(settings, "LLM_API_PROTOCOL", "auto") or "auto")
        self._g_web_search_mode = str(
            config.get("g_web_search_mode") or getattr(settings, "LLM_WEB_SEARCH_MODE", "local") or "local")
        # User-Agent 留空表示使用 SDK 默认值；只有从未配置过时才继承系统设置
        self._g_user_agent = str(
            config.get("g_user_agent") if config.get("g_user_agent") is not None
            else (getattr(settings, "LLM_USER_AGENT", "") or "")).strip()
        self._g_job_interval = max(0, self._to_int(
            config.get("g_job_interval"), self._to_int(getattr(settings, "AI_AGENT_JOB_INTERVAL", 0), 0)))
        self._discovery_api_key_runtime = ""

        self._runtime = self._load_runtime()
        self._mark_interrupted_states()
        # 失败计数只在当前进程/当前配置内有效：进程重启或配置重载后必须重新累计，
        # 否则一次历史故障会让新实例在首个 Agent 调用时直接接管。
        self._runtime["fail_count"] = 0
        self._runtime["fail_endpoint"] = ""
        self._runtime["takeover_id"] = ""
        self._runtime["failover_block_until"] = ""
        self._takeover_pending = False
        with self._lock:
            self._save_runtime()

        action = str(config.get("action") or "none").strip()
        new_profile_name = str(config.get("new_profile_name") or "").strip()
        action_message = ""
        try:
            if action == "save":
                action_message = self._do_save(new_profile_name)
            elif action == "discover":
                action_message = self._start_discovery()
            elif action == "apply_global":
                action_message = self._apply_global_options()
        except Exception as err:
            action_message = f"操作失败：{self._sanitize(err)}"
            logger.error(f"{self.plugin_name}：{action_message}")

        if self._apply_global and action != "apply_global":
            try:
                self._apply_global_options(silent=True)
            except Exception as err:  # noqa: BLE001
                logger.error(f"{self.plugin_name}：应用全局参数失败 - {err}")

        self._save_persistent_config(action_message=action_message)
        self._report_install()

    # ------------------------------------------------------------------
    # 旧 ID 迁移
    # ------------------------------------------------------------------

    def _migrate_legacy_id(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        把旧版大写插件 ID 下的配置与数据迁移到当前小写 ID。

        迁移必须满足两个前提：当前 ID 还没有配置、当前 ID 的每类数据为空。
        迁移完成后把标记写进旧 ID 的配置，而不是只写当前 ID 的 runtime：
        宿主重置插件会删除当前 ID 的配置与数据，若标记只存在 runtime 里，
        重置后旧配置和旧 API Key 会被再次迁移回来。
        """
        legacy_id = self._LEGACY_PLUGIN_ID
        if self.is_clone or not legacy_id or legacy_id == self.__class__.__name__:
            return config
        try:
            legacy_config = self.get_config(plugin_id=legacy_id) or {}
        except Exception as err:  # noqa: BLE001
            logger.error(f"{self.plugin_name}：读取旧插件配置失败 - {err}")
            return config
        if not isinstance(legacy_config, dict):
            legacy_config = {}
        if legacy_config.get(self._LEGACY_MIGRATED_KEY):
            return config
        # 兼容 2.7.x 早期版本只写 runtime 的迁移标记；同时把新标记补写进
        # 旧配置，保证之后重置当前插件时旧数据不会再次复活。
        runtime = self.get_data(self.DATA_KEY_RUNTIME)
        if isinstance(runtime, dict) and runtime.get("legacy_migrated_at"):
            if legacy_config and not legacy_config.get(self._LEGACY_MIGRATED_KEY):
                try:
                    marker = dict(legacy_config)
                    marker[self._LEGACY_MIGRATED_KEY] = self._now()
                    self.update_config(marker, plugin_id=legacy_id)
                except Exception as err:  # noqa: BLE001
                    logger.warning(f"{self.plugin_name}：补写旧插件迁移标记失败 - {err}")
            return config

        try:
            # 只在当前 ID 完全没有配置时接管旧配置，绝不覆盖当前 ID 已有配置。
            if legacy_config and not config:
                config = dict(legacy_config)
                self.update_config(config)
                logger.info(f"{self.plugin_name}：已从旧插件 ID [{legacy_id}] 迁移配置")

            # 数据迁移：每一类数据都只在当前 ID 为空时接管，避免覆盖新数据。
            migrated_keys = []
            for key in (self.DATA_KEY_PROFILES, self.DATA_KEY_RUNTIME, self.DATA_KEY_DISCOVERY):
                if self.get_data(key):
                    continue
                legacy_value = self.get_data(key, plugin_id=legacy_id)
                if not legacy_value:
                    continue
                self.save_data(key, legacy_value)
                migrated_keys.append(key)
            if migrated_keys:
                logger.info(f"{self.plugin_name}：已从旧插件 ID [{legacy_id}] 迁移数据 {migrated_keys}")

            # 标记写在旧 ID 配置上，宿主重置新插件时不会删除它。
            marker = dict(legacy_config)
            marker[self._LEGACY_MIGRATED_KEY] = self._now()
            self.update_config(marker, plugin_id=legacy_id)
            runtime = self.get_data(self.DATA_KEY_RUNTIME)
            runtime = dict(runtime) if isinstance(runtime, dict) else {}
            runtime["legacy_migrated_at"] = self._now()
            runtime["legacy_plugin_id"] = legacy_id
            self.save_data(self.DATA_KEY_RUNTIME, runtime)
        except Exception as err:  # noqa: BLE001
            logger.error(f"{self.plugin_name}：旧插件 ID 数据迁移失败 - {err}")
        return config

    # ------------------------------------------------------------------
    # 安装统计上报
    # ------------------------------------------------------------------

    def _report_install(self):
        """
        上报本插件的安装统计。

        MoviePilot 只在「通过插件仓库安装成功」时上报一次安装统计，本地部署、
        手动放置文件或旧版本存量插件都不会计入，插件市场因此拿不到下载量。
        这里在插件加载时按版本补报一次，失败后按间隔重试。
        """
        report = (self._runtime.get("install_report") or {}) if isinstance(self._runtime, dict) else {}
        if report.get("version") == self.plugin_version and report.get("ok"):
            return
        last_try = str(report.get("last_try_at") or "")
        if not report.get("ok") and last_try:
            try:
                elapsed = (datetime.now() - datetime.strptime(last_try, "%Y-%m-%d %H:%M:%S")).total_seconds()
                if elapsed < self._REPORT_RETRY_INTERVAL and report.get("version") == self.plugin_version:
                    return
            except ValueError:
                pass
        threading.Thread(
            target=self._report_install_worker,
            args=(self._generation, self._stop_event),
            daemon=True,
        ).start()

    def _report_install_worker(self, generation: int, stop_event: threading.Event):
        """后台执行安装统计上报，避免阻塞插件加载。"""
        record: Dict[str, Any] = {
            "version": self.plugin_version,
            "last_try_at": self._now(),
            "ok": False,
            "message": "",
        }
        try:
            if not getattr(settings, "PLUGIN_STATISTIC_SHARE", True):
                record["message"] = "系统已关闭插件安装统计共享"
                logger.info(f"{self.plugin_name}：{record['message']}，跳过安装统计上报")
            else:
                from app.adapters.external.server import MoviePilotServerHelper

                ok = MoviePilotServerHelper.install_plugin_reg(
                    plugin_id=self.__class__.__name__,
                    repo_url=self.plugin_repo_url,
                )
                record["ok"] = bool(ok)
                record["message"] = "上报成功" if ok else "上报失败或服务端未接受"
                if ok:
                    logger.info(f"{self.plugin_name}：安装统计已上报（v{self.plugin_version}）")
                else:
                    logger.warn(f"{self.plugin_name}：安装统计上报未成功，稍后重试")
        except Exception as err:  # noqa: BLE001
            record["message"] = str(err)[:180]
            logger.warn(f"{self.plugin_name}：安装统计上报异常 - {err}")
        if self._is_cancelled(generation, stop_event):
            return
        with self._lock:
            self._runtime["install_report"] = record
            self._save_runtime()

    # ------------------------------------------------------------------
    # 基础
    # ------------------------------------------------------------------

    def _save_persistent_config(self, action_message: str = ""):
        """持久化配置并清空一次性动作字段，避免重复执行。"""
        current = self.get_config() or {}
        payload = {
            "_active_tab": self._active_tab,
            "enabled": self._enabled,
            "include_credentials": self._include_credentials,
            "notify": self._notify,
            "auto_failover": self._auto_failover,
            "auto_recover": self._auto_recover,
            "takeover": self._takeover,
            "check_cron": self._check_cron,
            "fail_threshold": self._fail_threshold,
            "test_timeout": self._test_timeout,
            "probe_prompt": self._probe_prompt,
            "discover_base_url": self._discover_base_url,
            "discover_api_key": self._discover_api_key,
            "discover_providers": self._discover_providers,
            "discover_filter": self._discover_filter,
            "discover_limit": self._discover_limit,
            "discover_auto_import": self._discover_auto_import,
            "discover_smart_match": self._discover_smart_match,
            "whitelist": ", ".join(self._whitelist),
            "apply_global": self._apply_global,
            "g_thinking_level": self._g_thinking_level,
            "g_use_proxy": self._g_use_proxy,
            "g_api_protocol": self._g_api_protocol,
            "g_web_search_mode": self._g_web_search_mode,
            "g_user_agent": self._g_user_agent,
            "g_job_interval": self._g_job_interval,
            "g_image_input": self._g_image_input,
            "g_audio_input": self._g_audio_input,
            "g_audio_output": self._g_audio_output,
            "g_retry_transfer": self._g_retry_transfer,
            "g_ai_recommend": self._g_ai_recommend,
            "action": "none",
            "new_profile_name": "",
        }
        if action_message:
            payload["last_action_message"] = action_message
            payload["last_action_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            payload["last_action_message"] = current.get("last_action_message") or ""
            payload["last_action_at"] = current.get("last_action_at") or ""
        self.update_config(payload)

    def get_state(self) -> bool:
        return bool(getattr(self, "_enabled", False))

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        """注册定时探活服务。"""
        if not self._enabled or not self._auto_failover or not self._check_cron:
            return []
        return [{
            "id": "agentconfigprofile_health_check",
            "name": "智能助手配置探活与自动切换",
            "trigger": CronTrigger.from_crontab(self._check_cron),
            "func": self.health_check_job,
            "kwargs": {},
        }]

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {"path": "/save_current", "endpoint": self.api_save_current, "methods": ["GET", "POST"],
             "summary": "保存当前配置为模板"},
            {"path": "/apply", "endpoint": self.api_apply, "methods": ["GET", "POST"], "summary": "应用模板"},
            {"path": "/probe", "endpoint": self.api_probe, "methods": ["GET", "POST"], "summary": "探活配置"},
            {"path": "/probe_all", "endpoint": self.api_probe_all, "methods": ["GET", "POST"], "summary": "探活全部模板"},
            {"path": "/delete", "endpoint": self.api_delete, "methods": ["GET", "POST"], "summary": "删除模板"},
            {"path": "/move", "endpoint": self.api_move, "methods": ["GET", "POST"], "summary": "调整模板顺序"},
            {"path": "/set_page", "endpoint": self.api_set_page, "methods": ["GET"], "summary": "切换模板列表分页"},
            {"path": "/failover_now", "endpoint": self.api_failover_now, "methods": ["GET", "POST"], "summary": "立即执行故障切换检查"},
            {"path": "/clear_log", "endpoint": self.api_clear_log, "methods": ["GET", "POST"], "summary": "清空切换日志"},
            {"path": "/noop", "endpoint": self.api_noop, "methods": ["GET"], "summary": "仅刷新页面数据"},
            {"path": "/discover", "endpoint": self.api_discover, "methods": ["GET", "POST"], "summary": "探测端点可用模型"},
            {"path": "/import_discovered", "endpoint": self.api_import_discovered, "methods": ["GET", "POST"],
             "summary": "导入探测到的模型为模板"},
            {"path": "/add_model", "endpoint": self.api_add_model, "methods": ["GET", "POST"], "summary": "把单个模型加为模板"},
            {"path": "/clear_discovery", "endpoint": self.api_clear_discovery, "methods": ["GET", "POST"],
             "summary": "清除探测结果"},
            {"path": "/normalize_names", "endpoint": self.api_normalize_names, "methods": ["GET", "POST"],
             "summary": "规范模板名称"},
            {"path": "/prune_offlist", "endpoint": self.api_prune_offlist, "methods": ["GET", "POST"],
             "summary": "清理白名单外的探测模板"},
            {"path": "/apply_global", "endpoint": self.api_apply_global, "methods": ["GET", "POST"],
             "summary": "应用全局参数到所有模板"},
        ]

    def stop_service(self):
        """插件停用或重载时通知后台线程退出，避免旧实例覆盖新实例的数据。"""
        try:
            self._stop_event.set()
        except Exception:  # noqa: BLE001
            pass

    @staticmethod
    def _to_int(value: Any, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @classmethod
    def _valid_cron(cls, cron: Any) -> str:
        """校验 cron 表达式，无效时回退默认值。"""
        text = str(cron or "").strip()
        if not text:
            return cls._DEFAULT_CRON
        try:
            CronTrigger.from_crontab(text)
            return text
        except Exception:
            logger.warn(f"{cls.plugin_name}：cron 表达式无效 [{text}]，回退 {cls._DEFAULT_CRON}")
            return cls._DEFAULT_CRON

    @staticmethod
    def _now() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _is_cancelled(
            self,
            generation: Optional[int] = None,
            stop_event: Optional[threading.Event] = None,
    ) -> bool:
        """判断后台任务是否属于已停止或已被新配置替换的实例代次。"""
        event = stop_event if stop_event is not None else getattr(self, "_stop_event", None)
        if event is not None and event.is_set():
            return True
        return generation is not None and generation != getattr(self, "_generation", generation)

    @classmethod
    def _provider_spec(cls, provider: Any):
        """按 provider id 获取系统内置的供应商定义，取不到返回 None。"""
        pid = str(provider or "").strip()
        if not pid:
            return None
        try:
            from app.agent.llm.provider import LLMProviderManager

            return LLMProviderManager().get_provider(pid)
        except Exception:  # noqa: BLE001
            return None

    @classmethod
    def _provider_label(cls, provider: Any) -> str:
        """返回供应商展示名，优先用系统定义，其次用探测协议名，最后用原始 id。"""
        pid = str(provider or "").strip()
        if not pid:
            return "-"
        spec = cls._provider_spec(pid)
        name = getattr(spec, "name", "") if spec else ""
        return name or cls._DISCOVER_PROVIDERS.get(pid, pid)

    @classmethod
    def _provider_needs_api_key(cls, provider: Any) -> bool:
        """判断该供应商是否必须配置 API Key（OAuth 类供应商可不填）。"""
        spec = cls._provider_spec(provider)
        if spec is None:
            return str(provider or "") not in {"chatgpt", "github-copilot"}
        if getattr(spec, "oauth_methods", ()):
            return False
        return bool(getattr(spec, "supports_api_key", True))

    # ------------------------------------------------------------------
    # 数据存取
    # ------------------------------------------------------------------

    def _load_profiles(self) -> List[Dict[str, Any]]:
        data = self.get_data(self.DATA_KEY_PROFILES)
        if isinstance(data, list):
            return data
        return []

    def _save_profiles(self, profiles: List[Dict[str, Any]]):
        with self._lock:
            self.save_data(self.DATA_KEY_PROFILES, profiles)

    def _load_runtime(self) -> Dict[str, Any]:
        data = self.get_data(self.DATA_KEY_RUNTIME)
        if not isinstance(data, dict):
            data = {}
        data.setdefault("fail_count", 0)
        data.setdefault("fail_endpoint", "")
        data.setdefault("failover_block_until", "")
        data.setdefault("log", [])
        data.setdefault("last_check_at", "")
        data.setdefault("last_check_ok", None)
        data.setdefault("last_check_message", "")
        data.setdefault("last_check_duration", 0)
        data.setdefault("takeover_id", "")
        data.setdefault("page", 1)
        return data

    def _save_runtime(self):
        with self._lock:
            self.save_data(self.DATA_KEY_RUNTIME, self._runtime)

    def _mark_interrupted_states(self):
        """
        把上代后台任务遗留的 running 状态改写为中断。

        init_plugin 会先停掉旧代任务再执行到这里，因此此刻仍处于 running 的
        探活/探测必然没能落终态（进程重启或保存配置触发重载）；若不改写，
        任务入口会一直判定“正在进行中”，进度面板也会永久停在旧进度。
        """
        dirty = False
        progress = self._runtime.get("probe_all")
        if isinstance(progress, dict) and progress.get("status") == "running":
            progress["status"] = "error"
            progress["message"] = "任务已中断（插件重载或进程重启）"
            progress["current"] = ""
            progress["finished_at"] = self._now()
            dirty = True
        discovery = self._load_discovery()
        if discovery.get("status") == "running":
            discovery["status"] = "error"
            discovery["message"] = "任务已中断（插件重载或进程重启）"
            discovery["finished_at"] = self._now()
            self._save_discovery(discovery)
        if dirty:
            self._save_runtime()

    def _add_log(self, text: str, level: str = "info"):
        """记录一条切换/探活日志。"""
        with self._lock:
            logs = self._runtime.setdefault("log", [])
            logs.insert(0, {"time": self._now(), "text": self._sanitize(text), "level": level})
            del logs[self._LOG_MAX:]

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

    # ------------------------------------------------------------------
    # 配置快照
    # ------------------------------------------------------------------

    def _capture_snapshot(self) -> Dict[str, Any]:
        """捕获当前智能助手配置为快照。"""
        with self._apply_lock:
            llm: Dict[str, Any] = {}
            for key in self._LLM_SETTING_KEYS:
                if key in self._SENSITIVE_SETTING_KEYS and not self._include_credentials:
                    continue
                if hasattr(settings, key):
                    llm[key] = getattr(settings, key)
            return {"snapshot": {"llm": llm}}

    def _current_llm_config(self) -> Dict[str, Any]:
        """读取当前生效的 LLM 配置。"""
        with self._apply_lock:
            return {key: getattr(settings, key, None)
                    for key in self._LLM_SETTING_KEYS if hasattr(settings, key)}

    @staticmethod
    def _profile_llm(profile: Dict[str, Any]) -> Dict[str, Any]:
        return ((profile or {}).get("snapshot") or {}).get("llm") or {}

    def _effective_llm(self, llm: Dict[str, Any]) -> Dict[str, Any]:
        """返回本次实际操作真正会使用的配置；关闭凭据保存时忽略旧模板密钥。"""
        data = dict(llm or {})
        if not self._include_credentials:
            data.pop("LLM_API_KEY", None)
        return data

    def _sanitize(self, text: Any) -> str:
        """清洗错误/日志中的密钥与 Authorization 头，避免二次泄露。"""
        value = str(text or "")
        value = re.sub(r"(?i)(authorization\s*:\s*bearer\s+)[^\s,;]+", r"\1***", value)
        value = re.sub(r"(?i)([?&](?:api[_-]?key|apikey|token)=)[^&\s]+", r"\1***", value)
        secrets = (
            str(getattr(settings, "LLM_API_KEY", "") or ""),
            str(getattr(settings, "API_TOKEN", "") or ""),
            str(getattr(self, "_discover_api_key", "") or ""),
            str(getattr(self, "_discovery_api_key_runtime", "") or ""),
        )
        for secret in secrets:
            if len(secret) >= 4:
                value = value.replace(secret, "***")
        return value

    def _update_settings_checked(self, updates: Dict[str, Any], label: str) -> None:
        """逐项写入系统设置，失败时尽力回滚本次已成功写入的项目。"""
        if not updates:
            return
        previous = {key: getattr(settings, key, None) for key in updates}
        results = settings.update_settings(env=updates)
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
                settings.update_settings(env=rollback)
            except Exception as error:  # noqa: BLE001
                logger.error(f"{self.plugin_name}：{label}失败后的回滚异常 - {error}")
        raise RuntimeError(f"{label}失败：{', '.join(failed)}")

    def _apply_llm_config(self, llm: Dict[str, Any]) -> bool:
        """将 LLM 配置写回系统设置。"""
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
        """生成用于运行状态绑定的稳定指纹（包含密钥哈希，不落明文）。"""
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

    # ------------------------------------------------------------------
    # 探活
    # ------------------------------------------------------------------

    @staticmethod
    def _run_coro(factory) -> Any:
        """在同步上下文中安全执行协程工厂。"""
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
            if api_key and len(api_key) > 6:
                message = self._sanitize(message)
            # 宿主的 LLMTestError/LLMTestTimeout 会带上已耗时 duration_ms
            return False, message[:180], self._to_int(getattr(err, "duration_ms", 0), 0)

        duration = self._to_int((result or {}).get("duration_ms"), 0)
        if not (result or {}).get("reply_preview"):
            return False, "模型响应为空", duration
        return True, "OK", duration

    def _record_profile_health(self, profile: Dict[str, Any], ok: bool, message: str, duration: int):
        """把探活结果写入模板。"""
        health = profile.setdefault("health", {})
        health["status"] = "ok" if ok else "fail"
        health["checked_at"] = self._now()
        health["message"] = message
        health["duration_ms"] = duration
        health["fail_count"] = 0 if ok else self._to_int(health.get("fail_count"), 0) + 1

    def _probe_profile(
            self,
            profile_id: str,
            generation: Optional[int] = None,
            stop_event: Optional[threading.Event] = None,
    ) -> Tuple[bool, str]:
        """探活单个模板并持久化结果。"""
        with self._lock:
            profiles = self._load_profiles()
            profile = self._find_by_id(profiles, profile_id)
            if not profile:
                return False, "模板不存在"
            llm = self._effective_llm(self._profile_llm(profile))
            fingerprint = self._fingerprint(llm)
            name = profile.get("name") or profile_id
        ok, message, duration = self._probe(llm)
        if self._is_cancelled(generation, stop_event):
            return False, "探活任务已停止"
        with self._lock:
            profiles = self._load_profiles()
            profile = self._find_by_id(profiles, profile_id)
            if profile and self._fingerprint(
                    self._effective_llm(self._profile_llm(profile))) == fingerprint:
                self._record_profile_health(profile, ok, message, duration)
                self._save_profiles(profiles)
            elif profile:
                # 探活期间模板被更新：旧结果不能覆盖新快照。
                text = f"模板 [{name}] 在探活期间已变更，本次结果已忽略"
                logger.info(f"{self.plugin_name}：{text}")
                return False, text
        text = f"模板 [{name}] 探活{'成功' if ok else '失败'}：{message}"
        logger.info(f"{self.plugin_name}：{text}")
        return ok, text

    def _probe_current(
            self,
            generation: Optional[int] = None,
            stop_event: Optional[threading.Event] = None,
    ) -> Tuple[bool, str, int]:
        """探活当前生效配置并写入运行状态。"""
        llm = self._current_llm_config()
        fingerprint = self._fingerprint(llm)
        ok, message, duration = self._probe(llm)
        if self._is_cancelled(generation, stop_event):
            return False, "探活任务已停止", duration
        latest = self._current_llm_config()
        if self._fingerprint(latest) != fingerprint:
            return False, "当前配置已变更，本次探活结果已忽略", duration
        with self._lock:
            self._runtime["last_check_at"] = self._now()
            self._runtime["last_check_ok"] = ok
            self._runtime["last_check_message"] = message
            self._runtime["last_check_duration"] = duration
            if ok:
                self._runtime["fail_count"] = 0
                self._runtime["fail_endpoint"] = ""
            else:
                self._runtime["fail_count"] = self._to_int(self._runtime.get("fail_count"), 0) + 1
                self._runtime["fail_endpoint"] = fingerprint
            # 同步写入同端点模板的健康状态
            profiles = self._load_profiles()
            changed = False
            for profile in profiles:
                if self._same_endpoint(self._effective_llm(self._profile_llm(profile)), latest):
                    self._record_profile_health(profile, ok, message, duration)
                    changed = True
            if changed:
                self._save_profiles(profiles)
            self._save_runtime()
        return ok, message, duration

    def _start_probe_all(self) -> str:
        """后台并发探活全部模板，每完成一个立即写入进度，页面刷新即可看到最新结果。"""
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
        """探活当前配置与所有模板，多线程并发执行并逐个刷新进度。"""
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

    # ------------------------------------------------------------------
    # 动作实现
    # ------------------------------------------------------------------

    def _global_llm_values(self) -> Dict[str, Any]:
        """返回需要统一到所有模板的 LLM 参数。"""
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
        """返回与模型无关、只写入系统设置的全局参数。"""
        return {
            "AI_AGENT_RETRY_TRANSFER": self._g_retry_transfer,
            "AI_RECOMMEND_ENABLED": self._g_ai_recommend,
            "AI_AGENT_JOB_INTERVAL": self._g_job_interval,
        }

    def _overlay_global(self, llm: Dict[str, Any]) -> Dict[str, Any]:
        """把全局参数覆盖到一份 LLM 配置上（未启用时原样返回）。"""
        if not self._apply_global:
            return llm
        merged = dict(llm or {})
        merged.update(self._global_llm_values())
        return merged

    def _apply_global_options(self, silent: bool = False) -> str:
        """把全局参数写入系统设置，并统一覆盖所有模板快照。"""
        llm_values = self._global_llm_values()
        env_updates = {**llm_values, **self._global_agent_values()}

        changed_settings = [key for key, value in env_updates.items()
                            if getattr(settings, key, None) != value]
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
        """将当前智能助手配置保存为命名模板（同名覆盖）。"""
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
        """应用指定模板，将其配置写回智能助手并立即生效。"""
        with self._apply_lock:
            if background and (not getattr(self, "_enabled", False) or self._is_cancelled(generation, stop_event)):
                return "自动切换已停止"
            with self._lock:
                profiles = self._load_profiles()
                profile = self._find_by_id(profiles, profile_id)
                if not profile:
                    raise RuntimeError("应用失败：模板不存在")
                llm = self._effective_llm(self._profile_llm(profile))
                name = profile.get("name")
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
            logger.info(f"{self.plugin_name}：已切换到模板 [{name}] {reason}")
            return f"已切换到模板 [{name}]"

    def _do_delete(self, profile_id: str) -> str:
        with self._lock:
            profiles = self._load_profiles()
            profile = self._find_by_id(profiles, profile_id)
            if not profile:
                return "删除失败：模板不存在"
            profiles = [item for item in profiles if item.get("id") != profile_id]
            self._save_profiles(profiles)
        logger.info(f"{self.plugin_name}：已删除模板 [{profile.get('name')}]")
        return f"已删除模板 [{profile.get('name')}]"

    def _do_move(self, profile_id: str, direction: str) -> str:
        """调整模板在故障切换顺序中的位置。"""
        with self._lock:
            profiles = self._load_profiles()
            index = next((i for i, item in enumerate(profiles) if item.get("id") == profile_id), -1)
            if index < 0:
                return "调整失败：模板不存在"
            target = index - 1 if direction == "up" else index + 1
            if target < 0 or target >= len(profiles):
                return "已在边界，无需调整"
            profiles[index], profiles[target] = profiles[target], profiles[index]
            self._save_profiles(profiles)
        return "顺序已调整"

    def _total_pages(self, total: int) -> int:
        """按分页大小计算总页数。"""
        return max(1, (max(0, total) + self._PAGE_SIZE - 1) // self._PAGE_SIZE)

    def _current_page(self, total: int) -> int:
        """返回当前有效页码（自动收敛到合法范围）。"""
        page = self._to_int((self._runtime or {}).get("page"), 1)
        return min(max(1, page), self._total_pages(total))

    def _set_page(self, page: Any) -> str:
        """记录模板列表当前页码。"""
        total = len(self._load_profiles())
        target = min(max(1, self._to_int(page, 1)), self._total_pages(total))
        with self._lock:
            self._runtime["page"] = target
            self._save_runtime()
        return f"已切换到第 {target}/{self._total_pages(total)} 页"

    # ------------------------------------------------------------------
    # 模型探测与自动建模板
    # ------------------------------------------------------------------

    def _load_discovery(self) -> Dict[str, Any]:
        with self._lock:
            data = self.get_data(self.DATA_KEY_DISCOVERY)
            if isinstance(data, dict):
                if "api_key" in data:
                    data = dict(data)
                    data.pop("api_key", None)
                    self.save_data(self.DATA_KEY_DISCOVERY, data)
                return data
            return {}

    def _save_discovery(self, data: Dict[str, Any]):
        with self._lock:
            self.save_data(self.DATA_KEY_DISCOVERY, data)

    @staticmethod
    def _host_of(url: str) -> str:
        """从 base_url 中提取主机名，用于模板命名。"""
        text = str(url or "").strip()
        if not text:
            return "-"
        try:
            from urllib.parse import urlparse

            parsed = urlparse(text if "//" in text else f"//{text}")
            return parsed.hostname or text
        except Exception:
            return text

    def _match_filter(self, model_id: str) -> bool:
        """按用户设置的正则过滤模型。"""
        if not self._discover_filter:
            return True
        try:
            import re

            return bool(re.search(self._discover_filter, str(model_id or ""), re.IGNORECASE))
        except Exception:
            return True

    def _list_models(self, provider: str, base_url: str, api_key: str) -> Tuple[bool, str, List[Dict[str, Any]]]:
        """列出指定协议在该端点下可用的模型。"""
        from app.agent.llm.helper import LLMHelper

        try:
            models = self._run_coro(lambda: LLMHelper().get_models(
                provider=provider,
                api_key=api_key,
                base_url=base_url,
                use_proxy=bool(getattr(settings, "LLM_USE_PROXY", False)),
                force_refresh=True,
            ))
        except Exception as err:  # noqa: BLE001
            message = str(err) or err.__class__.__name__
            if api_key and len(api_key) > 6:
                message = self._sanitize(message)
            return False, message[:180], []

        records = []
        for item in models or []:
            model_id = str((item or {}).get("id") or "").strip()
            if not model_id:
                continue
            records.append({
                "id": model_id,
                "name": (item or {}).get("name") or model_id,
                "context_tokens_k": (item or {}).get("context_tokens_k"),
                "supports_reasoning": bool((item or {}).get("supports_reasoning")),
                "supports_image_input": bool((item or {}).get("supports_image_input")),
                "supports_audio_input": bool((item or {}).get("supports_audio_input")),
            })
        if not records:
            return False, "未返回任何模型", []
        return True, f"发现 {len(records)} 个模型", records[:200]

    def _resolve_discovery_key(self, base_url: str, api_key: str = "") -> str:
        """解析探测/导入使用的密钥，禁止把当前系统密钥发往不同端点。"""
        key = str(api_key or "").strip()
        if key:
            return key
        key = str(getattr(self, "_discovery_api_key_runtime", "") or "").strip()
        if key:
            return key
        key = str(self._discover_api_key or "").strip()
        if key:
            return key
        current = self._current_llm_config()
        if base_url and self._identity_text(base_url) == self._identity_text(
                current.get("LLM_BASE_URL")):
            return str(current.get("LLM_API_KEY") or "").strip()
        return ""

    def _start_discovery(self, base_url: str = "", api_key: str = "", pid: str = "") -> str:
        """启动后台探测任务，避免阻塞插件加载或页面请求。"""
        if pid:
            profile = self._find_by_id(self._load_profiles(), pid)
            if not profile:
                return "探测失败：模板不存在"
            llm = self._effective_llm(self._profile_llm(profile))
            profile_base_url = str(llm.get("LLM_BASE_URL") or "").strip()
            if base_url and self._identity_text(base_url) != self._identity_text(profile_base_url):
                # 不允许“拿模板 A 的 Key 去探测任意 B 地址”；需要换地址就显式填 Key。
                return "探测失败：为模板指定不同地址时不会复用其 API Key，请显式填写 API Key"
            base_url = base_url or profile_base_url
            api_key = api_key or str(llm.get("LLM_API_KEY") or "")
        base_url = (base_url or self._discover_base_url
                    or str(getattr(settings, "LLM_BASE_URL", "") or "")).strip()
        api_key = self._resolve_discovery_key(base_url, api_key)
        if not base_url:
            return "探测失败：请先填写 LLM 基础 URL"
        if "://" in base_url and not base_url.lower().startswith(("http://", "https://")):
            return "探测失败：仅支持 http/https 端点"
        if not api_key:
            return "探测失败：请先填写 API Key"

        with self._lock:
            discovery = self._load_discovery()
            if discovery.get("status") == "running":
                return "探测正在进行中，请稍后刷新查看结果"
            self._discovery_api_key_runtime = api_key
            generation = self._generation
            stop_event = self._stop_event
            self._save_discovery({
                "status": "running",
                "base_url": base_url,
                "started_at": self._now(),
                "providers": {},
            })
        threading.Thread(
            target=self._discovery_worker,
            args=(base_url, api_key, generation, stop_event),
            daemon=True,
        ).start()
        return f"已开始探测 {self._host_of(base_url)} 的可用模型，稍后在数据详情页查看结果"

    def _discovery_worker(
            self,
            base_url: str,
            api_key: str,
            generation: int,
            stop_event: threading.Event,
    ):
        """依次用各协议探测端点模型目录，可选自动导入模板。"""
        result: Dict[str, Any] = {
            "status": "running",
            "base_url": base_url,
            "started_at": self._now(),
            "providers": {},
        }
        try:
            for provider in self._discover_providers:
                if self._is_cancelled(generation, stop_event):
                    return
                ok, message, models = self._list_models(provider, base_url, api_key)
                if self._is_cancelled(generation, stop_event):
                    return
                result["providers"][provider] = {
                    "label": self._DISCOVER_PROVIDERS.get(provider, provider),
                    "ok": ok,
                    "message": message,
                    "count": len(models),
                    "models": models,
                }
                logger.info(f"{self.plugin_name}：探测 [{provider}] {self._host_of(base_url)} - {message}")
                self._save_discovery({**result, "status": "running"})
            result["status"] = "done"
            result["finished_at"] = self._now()
            if self._is_cancelled(generation, stop_event):
                return
            self._save_discovery(result)

            if self._discover_auto_import:
                if self._is_cancelled(generation, stop_event):
                    return
                import_message = self._import_discovered(api_key=api_key)
                result["import_message"] = import_message
                result["imported_at"] = self._now()
                if not self._is_cancelled(generation, stop_event):
                    self._save_discovery(result)
            if not self._is_cancelled(generation, stop_event):
                summary = "、".join(
                    f"{(info or {}).get('label') or pid}：{(info or {}).get('count') or 0} 个"
                    for pid, info in (result.get("providers") or {}).items()
                    if (info or {}).get("ok")
                ) or "没有协议探测成功"
                self._notify_safe(
                    "智能助手模型探测完成",
                    f"端点：{self._host_of(base_url)}\n{summary}\n"
                    f"{result.get('import_message') or '未自动导入模板'}",
                )
        except Exception as err:  # noqa: BLE001
            result["status"] = "error"
            result["message"] = self._sanitize(err)[:180]
            result["finished_at"] = self._now()
            if not self._is_cancelled(generation, stop_event):
                self._save_discovery(result)
            logger.error(f"{self.plugin_name}：模型探测失败 - {err}")
        finally:
            if generation == getattr(self, "_generation", generation) \
                    and self._is_cancelled(generation, stop_event):
                current = self._load_discovery()
                if current.get("status") == "running":
                    current["status"] = "error"
                    current["message"] = "任务已停止"
                    current["finished_at"] = self._now()
                    self._save_discovery(current)

    def _model_snapshot(self, provider: str, base_url: str, api_key: str,
                        model: Dict[str, Any]) -> Dict[str, Any]:
        """基于探测到的模型元数据构造模板快照。"""
        llm: Dict[str, Any] = {
            "LLM_PROVIDER": provider,
            "LLM_MODEL": str(model.get("id")),
            "LLM_BASE_URL": base_url,
            "LLM_BASE_URL_PRESET": None,
            "LLM_USER_AGENT": None,
            "LLM_USE_PROXY": bool(getattr(settings, "LLM_USE_PROXY", False)),
            # 未启用全局参数时，继承当前系统设置，避免新建模板被强制回到最保守值
            "LLM_THINKING_LEVEL": str(getattr(settings, "LLM_THINKING_LEVEL", "off") or "off"),
            "LLM_API_PROTOCOL": str(getattr(settings, "LLM_API_PROTOCOL", "auto") or "auto"),
            "LLM_WEB_SEARCH_MODE": str(getattr(settings, "LLM_WEB_SEARCH_MODE", "local") or "local"),
            "LLM_TEMPERATURE": float(getattr(settings, "LLM_TEMPERATURE", 0.3) or 0.3),
            "LLM_SUPPORT_IMAGE_INPUT": bool(model.get("supports_image_input")),
            "LLM_SUPPORT_AUDIO_INPUT": bool(model.get("supports_audio_input")),
            "LLM_SUPPORT_AUDIO_OUTPUT": False,
        }
        if self._include_credentials and api_key:
            llm["LLM_API_KEY"] = api_key
        context_k = self._to_int(model.get("context_tokens_k"), 0)
        llm["LLM_MAX_CONTEXT_TOKENS"] = context_k or self._to_int(
            getattr(settings, "LLM_MAX_CONTEXT_TOKENS", 256), 256)
        llm = self._overlay_global(llm)
        return {"snapshot": {"llm": llm}}

    def _profile_name_for(self, profiles: List[Dict[str, Any]], provider: str, base_url: str,
                          model_id: str) -> str:
        """生成带协议与站点信息且不冲突的模板名称。"""
        label = self._DISCOVER_PROVIDERS.get(provider, provider)
        base_name = f"{model_id} · {label} · {self._host_of(base_url)}"
        if not self._find_by_name(profiles, base_name):
            return base_name
        index = 2
        while self._find_by_name(profiles, f"{base_name} #{index}"):
            index += 1
        return f"{base_name} #{index}"

    @staticmethod
    def _parse_list(text: Any) -> List[str]:
        """把逗号/换行分隔的文本解析为去重列表。"""
        if isinstance(text, (list, tuple)):
            items = [str(item).strip() for item in text]
        else:
            raw = str(text or "")
            for sep in ("\n", "\r", "、", "；", ";", "|"):
                raw = raw.replace(sep, ",")
            items = [part.strip() for part in raw.split(",")]
        result = []
        for item in items:
            if item and item not in result:
                result.append(item)
        return result

    def _has_whitelist(self) -> bool:
        """是否配置了模型白名单。"""
        return bool(self._whitelist)

    def _preferred_provider(self, model_id: str, available: List[str]) -> Optional[str]:
        """按模型名推断最合适的协议，避免同一模型在多个协议下重复建模板。"""
        if not available:
            return None
        lower = str(model_id or "").lower()
        for provider, keywords in self._MODEL_PROVIDER_HINTS:
            if provider in available and any(key in lower for key in keywords):
                return provider
        for provider in self._PROVIDER_FALLBACK_ORDER:
            if provider in available:
                return provider
        return available[0]

    def _collect_discovered(self, discovery: Dict[str, Any],
                            provider_filter: str = "") -> Tuple[Dict[str, Dict[str, Dict[str, Any]]], List[str]]:
        """汇总探测结果为 模型 -> {协议: 元数据}。"""
        model_map: Dict[str, Dict[str, Dict[str, Any]]] = {}
        order: List[str] = []
        for provider, info in (discovery.get("providers") or {}).items():
            if provider_filter and provider != provider_filter:
                continue
            if not (info or {}).get("ok"):
                continue
            for model in (info.get("models") or []):
                model_id = str(model.get("id") or "")
                if not model_id:
                    continue
                if model_id not in model_map:
                    model_map[model_id] = {}
                    order.append(model_id)
                model_map[model_id][provider] = model
        return model_map, order

    def whitelist_status(self, discovery: Dict[str, Any]) -> Dict[str, List[str]]:
        """返回白名单在探测结果中的命中与未命中情况，供页面展示。"""
        model_map, _order = self._collect_discovered(discovery)
        known = {model_id.lower() for model_id in model_map}
        names = self._whitelist
        return {
            "hit": [name for name in names if name.lower() in known],
            "miss": [name for name in names if name.lower() not in known],
        }

    def _import_discovered(self, provider_filter: str = "", api_key: str = "") -> str:
        """把探测结果中的模型批量建成模板。白名单优先，其次智能匹配。"""
        discovery = self._load_discovery()
        base_url = discovery.get("base_url") or self._discover_base_url
        api_key = self._resolve_discovery_key(base_url, api_key)
        if not (discovery.get("providers") or {}):
            return "导入失败：没有探测结果"

        model_map, order = self._collect_discovered(discovery, provider_filter)
        if not model_map:
            return "导入完成：没有可用的探测结果"

        # 计划导入项：(协议, 模型ID, 模型元数据)
        plan: List[Tuple[str, str, Dict[str, Any]]] = []
        missed: List[str] = []
        use_whitelist = self._has_whitelist()
        if use_whitelist:
            lower_map = {mid.lower(): mid for mid in model_map}
            for name in self._whitelist:
                model_id = lower_map.get(name.lower())
                if not model_id:
                    missed.append(name)
                    continue
                # 同名模型可能在多个协议下都存在，按模型名智能匹配归属协议
                record = model_map[model_id]
                provider = self._preferred_provider(model_id, list(record))
                if not provider:
                    missed.append(name)
                    continue
                plan.append((provider, model_id, record[provider]))
        else:
            smart = self._discover_smart_match and not provider_filter
            for model_id in order:
                if not self._match_filter(model_id):
                    continue
                record = model_map[model_id]
                if smart:
                    provider = self._preferred_provider(model_id, list(record))
                    if provider:
                        plan.append((provider, model_id, record[provider]))
                else:
                    for provider, model in record.items():
                        plan.append((provider, model_id, model))

        added = 0
        skipped = 0
        per_provider: Dict[str, int] = {}
        needs_key = any(self._provider_needs_api_key(provider) for provider, _model_id, _model in plan)
        if needs_key and not api_key:
            return "导入失败：探测密钥已失效（插件重载后需重新探测或填写 API Key），已拒绝创建无密钥模板"
        with self._lock:
            profiles = self._load_profiles()
            for provider, model_id, model in plan:
                # 白名单是用户显式指定的清单，不再受每协议上限限制
                if not use_whitelist and self._discover_limit \
                        and per_provider.get(provider, 0) >= self._discover_limit:
                    continue
                candidate = {
                    "LLM_PROVIDER": provider,
                    "LLM_MODEL": model_id,
                    "LLM_BASE_URL": base_url,
                    "LLM_API_PROTOCOL": self._g_api_protocol if self._apply_global
                    else str(getattr(settings, "LLM_API_PROTOCOL", "auto") or "auto"),
                    "LLM_API_KEY": api_key,
                }
                duplicated = next(
                    (p for p in profiles if self._same_endpoint(
                        self._effective_llm(self._profile_llm(p)),
                        self._effective_llm(candidate))),
                    None,
                )
                if duplicated:
                    skipped += 1
                    continue
                profiles.append({
                    "id": uuid.uuid4().hex,
                    "name": self._profile_name_for(profiles, provider, base_url, model_id),
                    "created_at": self._now(),
                    "updated_at": self._now(),
                    "health": {"status": "unknown", "fail_count": 0},
                    "source": "discover",
                    **self._model_snapshot(provider, base_url, api_key, model),
                })
                per_provider[provider] = per_provider.get(provider, 0) + 1
                added += 1
            if added:
                self._save_profiles(profiles)

        text = f"已导入 {added} 个模板，跳过 {skipped} 个已存在"
        if use_whitelist:
            text += "（按白名单）"
            if missed:
                text += f"，白名单未探测到：{'、'.join(missed[:6])}"
        if added:
            with self._lock:
                self._add_log(text)
                self._save_runtime()
        logger.info(f"{self.plugin_name}：{text}")
        return text

    def _normalize_names(self, scope: str = "discover") -> str:
        """把模板名统一改成「模型 · 协议 · 站点」格式。"""
        renamed = 0
        with self._lock:
            profiles = self._load_profiles()
            for profile in profiles:
                if scope != "all" and profile.get("source") != "discover":
                    continue
                llm = self._profile_llm(profile)
                provider = llm.get("LLM_PROVIDER")
                model_id = llm.get("LLM_MODEL")
                if not provider or not model_id:
                    continue
                others = [item for item in profiles if item is not profile]
                new_name = self._profile_name_for(others, provider, llm.get("LLM_BASE_URL") or "", model_id)
                if new_name != profile.get("name"):
                    profile["name"] = new_name
                    profile["updated_at"] = self._now()
                    renamed += 1
            if renamed:
                self._save_profiles(profiles)
        text = f"已规范 {renamed} 个模板名称"
        logger.info(f"{self.plugin_name}：{text}（范围 {scope}）")
        return text

    def _prune_offlist(self) -> str:
        """删除探测生成但不在白名单内的模板。"""
        if not self._has_whitelist():
            return "未配置白名单，未做清理"
        allow = {name.lower() for name in self._whitelist}
        removed: List[str] = []
        with self._lock:
            profiles = self._load_profiles()
            kept = []
            for profile in profiles:
                if profile.get("source") != "discover":
                    kept.append(profile)
                    continue
                llm = self._profile_llm(profile)
                model_id = str(llm.get("LLM_MODEL") or "").lower()
                if model_id in allow:
                    kept.append(profile)
                    continue
                removed.append(profile.get("name") or model_id)
            if removed:
                self._save_profiles(kept)
        text = f"已清理 {len(removed)} 个名单外模板" if removed else "没有需要清理的名单外模板"
        if removed:
            self._add_log(text)
            self._save_runtime()
        logger.info(f"{self.plugin_name}：{text}")
        return text

    def _add_model_profile(self, provider: str, model_id: str) -> str:
        """把探测结果中的单个模型加为模板。"""
        discovery = self._load_discovery()
        base_url = discovery.get("base_url") or self._discover_base_url
        api_key = self._resolve_discovery_key(base_url)
        if self._provider_needs_api_key(provider) and not api_key:
            return "添加失败：探测密钥已失效，请重新探测或填写 API Key"
        info = (discovery.get("providers") or {}).get(provider) or {}
        model = next((m for m in (info.get("models") or []) if str(m.get("id")) == model_id), None)
        if not model:
            return "添加失败：探测结果中没有该模型"
        candidate = {
            "LLM_PROVIDER": provider,
            "LLM_MODEL": model_id,
            "LLM_BASE_URL": base_url,
            "LLM_API_PROTOCOL": self._g_api_protocol if self._apply_global
            else str(getattr(settings, "LLM_API_PROTOCOL", "auto") or "auto"),
            "LLM_API_KEY": api_key,
        }
        with self._lock:
            profiles = self._load_profiles()
            duplicated = next(
                (p for p in profiles if self._same_endpoint(
                    self._effective_llm(self._profile_llm(p)),
                    self._effective_llm(candidate))),
                None,
            )
            if duplicated:
                return f"模板已存在：{duplicated.get('name')}"
            name = self._profile_name_for(profiles, provider, base_url, model_id)
            profiles.append({
                "id": uuid.uuid4().hex,
                "name": name,
                "created_at": self._now(),
                "updated_at": self._now(),
                "health": {"status": "unknown", "fail_count": 0},
                "source": "discover",
                **self._model_snapshot(provider, base_url, api_key, model),
            })
            self._save_profiles(profiles)
        logger.info(f"{self.plugin_name}：已添加模板 [{name}]")
        return f"已添加模板 [{name}]"

    # ------------------------------------------------------------------
    # 自动切换
    # ------------------------------------------------------------------

    def _healthy_backup(self, current: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """按顺序返回第一个健康状态新鲜、密钥兼容且不是当前配置的模板。"""
        for profile in self._load_profiles():
            llm = self._effective_llm(self._profile_llm(profile))
            if self._same_endpoint(llm, current):
                continue
            if not self._health_is_fresh(profile):
                continue
            if not self._profile_key_compatible(llm, current):
                continue
            return profile
        return None

    def _notify_safe(self, title: str, text: str) -> None:
        """通知失败不得影响已经完成的切换或任务状态。"""
        if not self._notify:
            return
        try:
            self.post_message(mtype=NotificationType.Plugin, title=title, text=self._sanitize(text))
        except Exception as err:  # noqa: BLE001
            logger.warning(f"{self.plugin_name}：发送通知失败 - {err}")

    def _do_failover(
            self,
            reason: str,
            generation: Optional[int] = None,
            stop_event: Optional[threading.Event] = None,
    ) -> str:
        """串行执行故障切换，避免多个入口同时改写系统 LLM 配置。"""
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
        """按模板顺序寻找可用配置并切换。"""
        if not getattr(self, "_enabled", False) or self._is_cancelled(generation, stop_event):
            return "自动切换已停止"
        current = self._current_llm_config()
        profiles = self._load_profiles()
        candidates = [
            p for p in profiles
            if not self._same_endpoint(self._effective_llm(self._profile_llm(p)), current)
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

        # 新鲜健康的模板优先，其次未探活/已过期，最后是已知失败的。
        def rank(profile: Dict[str, Any]) -> int:
            if self._health_is_fresh(profile):
                return 0
            status = (profile.get("health") or {}).get("status")
            return 1 if status != "fail" else 2

        ordered = sorted(candidates, key=rank)
        tried = 0
        for profile in ordered:
            if self._is_cancelled(generation, stop_event):
                return "自动切换已停止"
            if self._FAILOVER_MAX_TRY and tried >= self._FAILOVER_MAX_TRY:
                break
            tried += 1
            try:
                ok, _text = self._probe_profile(profile.get("id"), generation, stop_event)
                if not ok:
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
        """当前配置正常但不是首选模板时，尝试切回首选模板。"""
        if not getattr(self, "_enabled", False) or self._is_cancelled(generation, stop_event):
            return None
        profiles = self._load_profiles()
        if not profiles:
            return None
        primary = profiles[0]
        primary_llm = self._effective_llm(self._profile_llm(primary))
        if self._same_endpoint(primary_llm, current):
            return None
        if not self._profile_key_compatible(primary_llm, current):
            return None
        try:
            if not self._health_is_fresh(primary):
                ok, _text = self._probe_profile(primary.get("id"), generation, stop_event)
                if not ok:
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
        """定时任务：探活当前配置，失效达阈值后自动切换。"""
        if not self._enabled:
            return "插件未启用"
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

    @eventmanager.register(ChainEventType.AgentLLMProvider, priority=60)
    def select_llm_provider(self, event: Event):
        """
        当前配置已被判定失效时，用备用模板即时接管本次 Agent 调用。
        """
        if not self.get_state() or not self._takeover or not event or not event.event_data:
            return
        if self._event_get(event.event_data, "selected_provider_id"):
            return
        if self._to_int(self._runtime.get("fail_count"), 0) < self._fail_threshold:
            return

        current = self._current_llm_config()
        # fail_count 必须属于当前配置；用户手工改回可用配置后不能继续接管。
        if self._runtime.get("fail_endpoint") != self._fingerprint(current):
            return
        backup = self._healthy_backup(current)
        if not backup:
            return
        llm = self._effective_llm(self._profile_llm(backup))
        if not self._profile_key_compatible(llm, current):
            return
        for setting_key, field in (
                ("LLM_PROVIDER", "provider"),
                ("LLM_MODEL", "model"),
                ("LLM_API_KEY", "api_key"),
                ("LLM_BASE_URL", "base_url"),
                ("LLM_USER_AGENT", "user_agent"),
                ("LLM_USE_PROXY", "use_proxy"),
                ("LLM_THINKING_LEVEL", "thinking_level"),
                ("LLM_API_PROTOCOL", "api_protocol"),
                ("LLM_WEB_SEARCH_MODE", "web_search_mode"),
        ):
            if setting_key in llm:
                self._event_set(event.event_data, field, llm.get(setting_key))
        # 预设 ID 必须跟随模板清除，否则宿主可能用旧预设覆盖模板的 base_url。
        self._event_set(event.event_data, "base_url_preset", llm.get("LLM_BASE_URL_PRESET"))
        self._event_set(event.event_data, "selected_provider_id", backup.get("id"))
        self._event_set(event.event_data, "selected_provider_name", backup.get("name"))
        self._event_set(event.event_data, "source", self.__class__.__name__)

        with self._lock:
            if self._runtime.get("takeover_id") != backup.get("id"):
                self._runtime["takeover_id"] = backup.get("id")
                self._add_log(f"当前配置失效，本次调用由模板 [{backup.get('name')}] 接管")
                # 事件契约不携带上下文上限等扩展字段，接管期间沿用系统设置的上下文上限
                current_ctx = self._to_int(getattr(settings, "LLM_MAX_CONTEXT_TOKENS", 0), 0)
                backup_ctx = self._to_int(llm.get("LLM_MAX_CONTEXT_TOKENS"), 0)
                if current_ctx and backup_ctx and current_ctx != backup_ctx:
                    self._add_log(f"接管模板上下文上限 {backup_ctx}k 与当前配置 {current_ctx}k 不同，"
                                  f"本次调用沿用当前配置的 {current_ctx}k")
                self._save_runtime()
        logger.info(f"{self.plugin_name}：当前配置失效，用模板 [{backup.get('name')}] 接管本次调用")

        if self._auto_failover:
            generation = self._generation
            stop_event = self._stop_event
            with self._lock:
                spawn = not self._takeover_pending
                if spawn:
                    self._takeover_pending = True
            if spawn:
                threading.Thread(
                    target=self._failover_in_background,
                    args=(f"当前模型失效，已由模板 [{backup.get('name')}] 接管", generation, stop_event),
                    daemon=True,
                ).start()

    def _failover_in_background(
            self,
            reason: str,
            generation: Optional[int] = None,
            stop_event: Optional[threading.Event] = None,
    ):
        """后台执行一次故障切换，避免阻塞事件链。"""
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

    @staticmethod
    def _event_get(event_data: Any, key: str, default: Any = None) -> Any:
        if isinstance(event_data, dict):
            return event_data.get(key, default)
        return getattr(event_data, key, default)

    @staticmethod
    def _event_set(event_data: Any, key: str, value: Any) -> None:
        if isinstance(event_data, dict):
            event_data[key] = value
        else:
            setattr(event_data, key, value)

    # ------------------------------------------------------------------
    # API
    # ------------------------------------------------------------------

    @staticmethod
    def _auth(apikey: str) -> bool:
        return apikey == settings.API_TOKEN

    def _api_run(self, apikey: str, func, *args, **kwargs) -> schemas.Response:
        """统一处理 API 鉴权与异常，避免任何单点异常让详情页整体报错。"""
        if not self._auth(apikey):
            return schemas.Response(success=False, message="认证失败")
        try:
            message = func(*args, **kwargs)
        except Exception as err:  # noqa: BLE001
            logger.error(f"{self.plugin_name}：接口执行失败 - {err}")
            return schemas.Response(success=False, message=f"操作失败：{self._sanitize(err)[:150]}")
        return schemas.Response(success=True, message=str(message or "已完成"))

    def api_save_current(self, apikey: str, name: str = ""):
        return self._api_run(apikey, self._do_save, str(name or "").strip())

    def api_apply(self, pid: str, apikey: str):
        return self._api_run(apikey, self._do_apply, pid, "手动切换")

    def api_probe(self, apikey: str, pid: str = ""):
        def run() -> str:
            if pid:
                _ok, text = self._probe_profile(pid)
                return text
            ok, message, duration = self._probe_current()
            return f"当前配置探活{'成功' if ok else '失败'}：{message}（{duration}ms）"

        return self._api_run(apikey, run)

    def api_probe_all(self, apikey: str):
        return self._api_run(apikey, self._start_probe_all)

    def api_delete(self, pid: str, apikey: str):
        return self._api_run(apikey, self._do_delete, pid)

    def api_move(self, pid: str, dir: str, apikey: str):
        return self._api_run(apikey, self._do_move, pid, dir)

    def api_set_page(self, apikey: str, p: str = "1"):
        """切换模板列表分页。"""
        return self._api_run(apikey, self._set_page, p)

    def api_failover_now(self, apikey: str):
        def run() -> str:
            with self._lock:
                self._runtime["failover_block_until"] = ""
            return self.health_check_job()

        return self._api_run(apikey, run)

    def api_clear_log(self, apikey: str):
        def run() -> str:
            with self._lock:
                self._runtime["log"] = []
                self._save_runtime()
            return "日志已清空"

        return self._api_run(apikey, run)

    def api_apply_global(self, apikey: str):
        return self._api_run(apikey, self._apply_global_options)

    def api_noop(self, apikey: str):
        """不做任何变更，仅让前端重新拉取页面数据。"""
        if not self._auth(apikey):
            return schemas.Response(success=False, message="认证失败")
        progress = (self._runtime or {}).get("probe_all") or {}
        discovery = self._load_discovery()
        if progress.get("status") == "running":
            return schemas.Response(
                success=True,
                message=f"探活进行中 {self._to_int(progress.get('done'), 0)}/"
                        f"{self._to_int(progress.get('total'), 0)}：{progress.get('current') or ''}")
        if discovery.get("status") == "running":
            return schemas.Response(success=True,
                                    message=f"模型探测进行中，已完成 {len(discovery.get('providers') or {})} 个协议")
        return schemas.Response(success=True, message="已刷新")

    def api_discover(self, apikey: str, base_url: str = "", api_key: str = "", pid: str = ""):
        return self._api_run(apikey, self._start_discovery, base_url, api_key, pid)

    def api_import_discovered(self, apikey: str, provider: str = ""):
        return self._api_run(apikey, self._import_discovered, provider)

    def api_add_model(self, provider: str, model: str, apikey: str):
        return self._api_run(apikey, self._add_model_profile, provider, model)

    def api_clear_discovery(self, apikey: str):
        def run() -> str:
            self._save_discovery({})
            return "探测结果已清除"

        return self._api_run(apikey, run)

    def api_normalize_names(self, apikey: str, scope: str = "discover"):
        return self._api_run(apikey, self._normalize_names, scope)

    def api_prune_offlist(self, apikey: str):
        return self._api_run(apikey, self._prune_offlist)

    # ------------------------------------------------------------------
    # 配置页（Vuetify）
    # ------------------------------------------------------------------

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """配置页按“基础 / 自动切换 / 模型探测”三个标签分区，模板操作在“数据详情”页完成。"""

        def caption(text: str) -> dict:
            return {"component": "div", "props": {"class": "text-caption text-medium-emphasis mb-2"}, "text": text}

        def switch(model: str, label: str, md: int = 4) -> dict:
            return {"component": "VCol", "props": {"cols": 12, "md": md},
                    "content": [{"component": "VSwitch", "props": {"model": model, "label": label}}]}

        def field(model: str, label: str, md: int = 4, **props) -> dict:
            item_props = {"model": model, "label": label}
            item_props.update(props)
            return {"component": "VCol", "props": {"cols": 12, "md": md},
                    "content": [{"component": "VTextField", "props": item_props}]}

        def row(*cols) -> dict:
            return {"component": "VRow", "props": {"dense": True}, "content": list(cols)}

        basic_tab = {
            "component": "div",
            "content": [
                caption("基本设置"),
                row(
                    switch("enabled", "启用插件", 4),
                    switch("include_credentials", "模板包含 API Key", 4),
                    switch("notify", "切换后发送通知", 4),
                ),
                {"component": "VDivider", "props": {"class": "my-3"}},
                caption("保存插件时执行的一次性动作"),
                row(
                    {"component": "VCol", "props": {"cols": 12, "md": 6},
                     "content": [{"component": "VSelect", "props": {
                         "model": "action", "label": "执行动作",
                         "items": [{"title": "（不执行）", "value": "none"},
                                   {"title": "把当前配置保存为模板", "value": "save"},
                                   {"title": "探测模型并导入模板", "value": "discover"}]}}]},
                    field("new_profile_name", "模板名称（保存当前配置时可填）", 6),
                ),
                {"component": "VAlert",
                 "props": {"type": "info", "variant": "tonal", "class": "mt-2", "density": "compact",
                           "text": "模板的切换、探活、排序、删除、导入都在插件的“数据详情”页一键完成。"
                                   "含凭据的模板会以明文保存 API Key。"}},
                {"component": "VAlert",
                 "props": {"type": "warning", "variant": "tonal", "class": "mt-2", "density": "compact",
                           "text": "关闭「模板包含 API Key」后模板只保存供应商、模型与地址；"
                                   "探活、切换和事件接管仅在供应商与端点一致时复用当前密钥，"
                                   "跨供应商或跨端点会被拒绝。"}},
            ],
        }

        failover_tab = {
            "component": "div",
            "content": [
                caption("切换开关"),
                row(
                    switch("auto_failover", "启用失效自动切换", 4),
                    switch("takeover", "失效期间即时接管调用", 4),
                    switch("auto_recover", "首选模板恢复后切回", 4),
                ),
                {"component": "VDivider", "props": {"class": "my-3"}},
                caption("探活参数"),
                row(
                    field("check_cron", "探活周期（cron）", 3, placeholder=self._DEFAULT_CRON),
                    field("fail_threshold", "连续失败次数阈值", 3, type="number", placeholder="2"),
                    field("test_timeout", "探活超时（秒）", 3, type="number", placeholder="20"),
                    field("probe_prompt", "探活提示词", 3, placeholder="请只回复 OK"),
                ),
                {"component": "VAlert",
                 "props": {"type": "warning", "variant": "tonal", "class": "mt-2", "density": "compact",
                           "text": "当前配置连续探活失败达到阈值后，按“数据详情”页的模板顺序依次探活，"
                                   "第一个可用的模板会写入系统设置并立即生效。"}},
            ],
        }

        discover_tab = {
            "component": "div",
            "content": [
                caption("端点信息（留空则使用当前生效的地址与密钥）"),
                row(
                    field("discover_base_url", "LLM 基础 URL", 6, placeholder="https://api.example.com/v1"),
                    field("discover_api_key", "API Key", 6, type="password", placeholder="sk-..."),
                ),
                {"component": "VDivider", "props": {"class": "my-3"}},
                caption("模型白名单（逗号或换行分隔；填写后只导入名单内的模型，协议按模型名自动匹配）"),
                row(
                    {"component": "VCol", "props": {"cols": 12, "md": 7},
                     "content": [{"component": "VTextarea",
                                  "props": {"model": "whitelist", "label": "模型白名单",
                                            "rows": 3, "auto-grow": True,
                                            "placeholder": "claude-opus-5, gpt-5.6-luna, deepseek-v4-pro"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 5},
                     "content": [{"component": "VSelect", "props": {
                         "model": "discover_providers", "label": "LLM 提供商（探测协议）",
                         "multiple": True, "chips": True,
                         "items": [{"title": label, "value": pid}
                                   for pid, label in self._DISCOVER_PROVIDERS.items()]}}]},
                ),
                {"component": "VDivider", "props": {"class": "my-3"}},
                caption("探测与导入参数"),
                row(
                    field("discover_filter", "模型名过滤（正则，白名单为空时生效）", 6,
                          placeholder="claude|gpt|deepseek"),
                    field("discover_limit", "每协议导入上限（0=不限）", 6, type="number", placeholder="20"),
                ),
                row(
                    switch("discover_auto_import", "探测后自动导入模板", 6),
                    switch("discover_smart_match", "按模型名智能匹配协议（白名单为空时生效）", 6),
                ),
                {"component": "VAlert",
                 "props": {"type": "info", "variant": "tonal", "class": "mt-2", "density": "compact",
                           "text": "按上方选中的 LLM 提供商（探测协议）请求该端点的模型目录。"
                                   "填了白名单就严格按名单建模板（同名模型按名称智能匹配归属协议，不受上限限制）；"
                                   "名单为空时才按正则过滤、智能匹配和每协议上限自动挑选。"
                                   "模板名格式为 模型 · 协议 · 站点。"}},
            ],
        }

        global_tab = {
            "component": "div",
            "content": [
                caption("全局参数（打开总开关后，这些值会写入系统设置，并统一覆盖所有模板）"),
                row(
                    switch("apply_global", "启用全局参数统一", 6),
                ),
                {"component": "VDivider", "props": {"class": "my-3"}},
                caption("模型能力开关"),
                row(
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VSelect", "props": {
                         "model": "g_thinking_level", "label": "思考模式",
                         "items": [{"title": title, "value": value}
                                   for value, title in self._THINKING_LEVEL_OPTIONS]}}]},
                    switch("g_use_proxy", "使用系统代理", 4),
                    switch("g_image_input", "模型支持图片输入", 4),
                ),
                row(
                    switch("g_audio_input", "支持音频输入", 4),
                    switch("g_audio_output", "支持音频输出", 4),
                ),
                {"component": "VDivider", "props": {"class": "my-3"}},
                caption("接口与调度参数"),
                row(
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VSelect", "props": {
                         "model": "g_api_protocol", "label": "API 协议",
                         "items": [{"title": title, "value": value}
                                   for value, title in self._API_PROTOCOL_OPTIONS]}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VSelect", "props": {
                         "model": "g_web_search_mode", "label": "联网搜索",
                         "items": [{"title": title, "value": value}
                                   for value, title in self._WEB_SEARCH_MODE_OPTIONS]}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VSelect", "props": {
                         "model": "g_job_interval", "label": "定时唤醒",
                         "items": [{"title": "不启用", "value": 0}] + [
                             {"title": f"{hours} 小时", "value": hours}
                             for hours in self._JOB_INTERVAL_OPTIONS]}}]},
                ),
                row(
                    field("g_user_agent", "User-Agent（留空使用 SDK 默认）", 12),
                ),
                {"component": "VDivider", "props": {"class": "my-3"}},
                caption("智能助手功能开关（只写入系统设置，与模板无关）"),
                row(
                    switch("g_retry_transfer", "文件整理失败智能接管", 6),
                    switch("g_ai_recommend", "搜索结果智能推荐", 6),
                ),
                {"component": "VAlert",
                 "props": {"type": "info", "variant": "tonal", "class": "mt-2", "density": "compact",
                           "text": "打开总开关并保存后立即生效：思考模式、系统代理、API 协议、联网搜索、User-Agent、"
                                   "图片/音频支持会覆盖到所有已保存模板与后续新建模板，切换模板时也会用这里的值，"
                                   "不会被旧模板快照覆盖回去；定时唤醒、整理失败智能接管与搜索智能推荐只写入系统设置。"
                                   "也可以在“数据详情”页点“应用全局参数”手动执行一次。"}},
            ],
        }

        form = [
            {
                "component": "VTabs",
                "props": {"model": "_active_tab", "color": "primary", "grow": True, "class": "mb-4"},
                "content": [
                    {"component": "VTab", "props": {"value": "basic"}, "text": "基础"},
                    {"component": "VTab", "props": {"value": "failover"}, "text": "失效自动切换"},
                    {"component": "VTab", "props": {"value": "discover"}, "text": "模型探测"},
                    {"component": "VTab", "props": {"value": "global"}, "text": "全局参数"},
                ],
            },
            {
                "component": "VWindow",
                "props": {"model": "_active_tab"},
                "content": [
                    {"component": "VWindowItem", "props": {"value": "basic"}, "content": [basic_tab]},
                    {"component": "VWindowItem", "props": {"value": "failover"}, "content": [failover_tab]},
                    {"component": "VWindowItem", "props": {"value": "discover"}, "content": [discover_tab]},
                    {"component": "VWindowItem", "props": {"value": "global"}, "content": [global_tab]},
                ],
            },
        ]

        default_config = {
            "_active_tab": "basic",
            "enabled": False,
            "include_credentials": True,
            "notify": True,
            "auto_failover": False,
            "takeover": True,
            "auto_recover": False,
            "check_cron": self._DEFAULT_CRON,
            "fail_threshold": 2,
            "test_timeout": 20,
            "probe_prompt": "请只回复 OK",
            "discover_base_url": "",
            "discover_api_key": "",
            "discover_providers": list(self._DISCOVER_PROVIDERS),
            "discover_filter": "",
            "discover_limit": 20,
            "discover_auto_import": True,
            "discover_smart_match": True,
            "whitelist": "",
            "apply_global": False,
            "g_thinking_level": "off",
            "g_use_proxy": True,
            "g_api_protocol": "auto",
            "g_web_search_mode": "local",
            "g_user_agent": "",
            "g_job_interval": 0,
            "g_image_input": True,
            "g_audio_input": False,
            "g_audio_output": False,
            "g_retry_transfer": False,
            "g_ai_recommend": False,
            "action": "none",
            "new_profile_name": "",
        }
        return form, default_config

    # ------------------------------------------------------------------
    # 详情页（操作台）
    # ------------------------------------------------------------------

    def _api_btn(self, text: str, path: str, params: Dict[str, Any], color: str = "primary",
                 variant: str = "text", size: str = "x-small", title: str = "") -> dict:
        """构造一个直接调用插件 API 的按钮。"""
        query = dict(params or {})
        query["apikey"] = settings.API_TOKEN
        # 前端渲染器对 POST 会把 params 放进请求体，verify_apikey 只读 query/header，
        # 会导致鉴权失败跳转登录页；统一用 GET，apikey 保持在 query 中。
        method = "get"
        props = {"color": color, "variant": variant, "size": size, "class": "px-2",
                 "style": "text-transform: none; letter-spacing: 0;"}
        if title:
            props["title"] = title
        return {
            "component": "VBtn",
            "props": props,
            "text": text,
            "events": {"click": {"api": f"plugin/{self.__class__.__name__}/{path}",
                                 "method": method, "params": query}},
        }

    def _discovery_card(self, discovery: Dict[str, Any]) -> dict:
        """构造模型探测结果卡片：状态、白名单命中、可点击的模型清单。"""
        status = (discovery or {}).get("status") or ""
        base_url = (discovery or {}).get("base_url") or self._discover_base_url
        providers = (discovery or {}).get("providers") or {}
        use_whitelist = self._has_whitelist()

        head_chips = []
        status_map = {
            "running": ("info", "探测中"),
            "done": ("success", "探测完成"),
            "error": ("error", f"探测失败 {(discovery or {}).get('message') or ''}"[:50]),
        }
        if status in status_map:
            color, text = status_map[status]
            head_chips.append({"component": "VChip",
                               "props": {"size": "small", "color": color, "variant": "flat", "class": "me-2"},
                               "text": text})
        if base_url:
            head_chips.append({"component": "VChip",
                               "props": {"size": "small", "variant": "tonal", "class": "me-2"},
                               "text": self._host_of(base_url)})
        if (discovery or {}).get("finished_at"):
            head_chips.append({"component": "VChip",
                               "props": {"size": "small", "variant": "tonal", "class": "me-2"},
                               "text": discovery.get("finished_at")})
        head_chips.append({"component": "VChip",
                           "props": {"size": "small", "variant": "tonal",
                                     "color": "primary" if use_whitelist else "secondary", "class": "me-2"},
                           "text": "白名单模式" if use_whitelist else "自动挑选模式"})
        if use_whitelist:
            wl = self.whitelist_status(discovery or {})
            wl_hit = wl.get("hit") or []
            wl_miss = wl.get("miss") or []
            if wl_hit or wl_miss:
                head_chips.append({"component": "VChip",
                                   "props": {"size": "small", "color": "primary", "variant": "tonal",
                                             "class": "me-2"},
                                   "text": f"白名单命中 {len(wl_hit)}/{len(wl_hit) + len(wl_miss)}"})
            for name in wl_miss[:4]:
                head_chips.append({"component": "VChip",
                                   "props": {"size": "small", "color": "warning", "variant": "outlined",
                                             "class": "me-2"},
                                   "text": f"未探测到 {name}"})
            if len(wl_miss) > 4:
                head_chips.append({"component": "VChip",
                                   "props": {"size": "small", "variant": "tonal", "class": "me-2"},
                                   "text": f"…另有 {len(wl_miss) - 4} 个未探测到"})
        if (discovery or {}).get("import_message"):
            head_chips.append({"component": "VChip",
                               "props": {"size": "small", "color": "primary", "variant": "tonal", "class": "me-2"},
                               "text": str(discovery.get("import_message"))[:60]})

        body: List[dict] = []
        if not providers:
            body.append({"component": "VAlert",
                         "props": {"type": "info", "variant": "tonal", "class": "ma-3", "density": "compact",
                                   "text": "还没有探测结果。配置页「模型探测」标签里填好地址与密钥（留空则用当前生效的），"
                                           "再点这里的“重新探测”即可。"}})
        else:
            for provider, label in self._DISCOVER_PROVIDERS.items():
                info = providers.get(provider)
                if not info:
                    continue
                ok = bool(info.get("ok"))
                names = [n.lower() for n in self._whitelist]

                head_row = [
                    {"component": "VChip",
                     "props": {"size": "small", "color": "success" if ok else "error", "variant": "flat",
                               "class": "me-2"},
                     "text": label},
                    {"component": "span", "props": {"class": "text-caption text-medium-emphasis me-2"},
                     "text": str(info.get("message") or "")[:70]},
                ]
                if ok:
                    head_row.append(self._api_btn("导入该协议", "import_discovered", {"provider": provider},
                                                  color="primary", variant="tonal"))
                body.append({"component": "div",
                             "props": {"class": "d-flex flex-wrap align-center px-3 pt-3 pb-1", "style": "gap: 4px;"},
                             "content": head_row})

                models = info.get("models") or []
                if not models:
                    continue
                if use_whitelist and names:
                    listed = [m for m in models if str(m.get("id", "")).lower() in names]
                    others = [m for m in models if str(m.get("id", "")).lower() not in names]
                else:
                    listed, others = models[:24], models[24:]
                model_btns = [
                    self._api_btn(str(model.get("id")), "add_model",
                                  {"provider": provider, "model": str(model.get("id"))},
                                  color="primary", variant="tonal")
                    for model in listed
                ]
                if not use_whitelist or not names:
                    if others:
                        model_btns.append({"component": "VChip",
                                           "props": {"size": "x-small", "variant": "tonal"},
                                           "text": f"…另有 {len(others)} 个"})
                elif others:
                    model_btns.extend([
                        self._api_btn(str(model.get("id")), "add_model",
                                      {"provider": provider, "model": str(model.get("id"))},
                                      color="default", variant="outlined")
                        for model in others[:12]
                    ])
                    if len(others) > 12:
                        model_btns.append({"component": "VChip",
                                           "props": {"size": "x-small", "variant": "tonal"},
                                           "text": f"…名单外另有 {len(others) - 12} 个"})
                body.append({"component": "div",
                             "props": {"class": "d-flex flex-wrap px-3 pb-2", "style": "gap: 4px;"},
                             "content": model_btns})

        return {
            "component": "VCard",
            "props": {"class": "mb-4"},
            "content": [
                {"component": "VCardTitle",
                 "props": {"class": "text-subtitle-1 d-flex flex-wrap align-center justify-space-between"},
                 "content": [
                     {"component": "div", "props": {"class": "d-flex flex-wrap align-center"},
                      "content": [{"component": "span", "props": {"class": "me-3"}, "text": "模型探测"}] + head_chips},
                     {"component": "div", "props": {"class": "d-flex align-center", "style": "gap: 4px;"},
                      "content": [
                          self._api_btn("重新探测", "discover", {}, color="success", variant="text"),
                          self._api_btn("导入全部", "import_discovered", {}, color="primary", variant="text"),
                          self._api_btn("清除结果", "clear_discovery", {}, color="error", variant="text"),
                      ]},
                 ]},
                {"component": "VCardText", "props": {"class": "pa-0 pb-2"},
                 "content": ([{"component": "div",
                               "props": {"class": "px-3 pt-2 text-caption text-medium-emphasis"},
                               "text": "点击模型名即可把该模型加为模板（深色=白名单内，浅色=名单外）"}]
                             if providers else []) + body},
            ],
        }

    def _progress_card(self) -> Optional[dict]:
        """探测/探活进行中时展示进度条，并给出刷新入口。"""
        runtime = self._runtime or {}
        discovery = self._load_discovery()
        probe = runtime.get("probe_all") or {}

        rows: List[dict] = []
        busy = False

        if probe.get("status") == "running":
            busy = True
            total = max(1, self._to_int(probe.get("total"), 1))
            done = self._to_int(probe.get("done"), 0)
            rows.append({"component": "div", "props": {"class": "px-3 pt-3"}, "content": [
                {"component": "div", "props": {"class": "d-flex align-center justify-space-between mb-1"},
                 "content": [
                     {"component": "span", "props": {"class": "text-body-2"},
                      "text": f"正在探活：{probe.get('current') or ''}"},
                     {"component": "span", "props": {"class": "text-caption text-medium-emphasis"},
                      "text": f"{done}/{total}｜正常 {self._to_int(probe.get('ok'), 0)}"
                              f"｜失败 {self._to_int(probe.get('fail'), 0)}"},
                 ]},
                {"component": "VProgressLinear",
                 "props": {"model-value": round(done * 100 / total), "height": 6, "rounded": True,
                           "color": "info"}},
            ]})
        elif probe.get("status") == "done" and probe.get("finished_at"):
            rows.append({"component": "div", "props": {"class": "px-3 pt-3 text-caption text-medium-emphasis"},
                         "text": f"上次全量探活 {probe.get('finished_at')}：正常 "
                                 f"{self._to_int(probe.get('ok'), 0)} 个，失败 "
                                 f"{self._to_int(probe.get('fail'), 0)} 个"})

        if discovery.get("status") == "running":
            busy = True
            finished = len(discovery.get("providers") or {})
            total = max(1, len(self._discover_providers))
            rows.append({"component": "div", "props": {"class": "px-3 pt-3"}, "content": [
                {"component": "div", "props": {"class": "d-flex align-center justify-space-between mb-1"},
                 "content": [
                     {"component": "span", "props": {"class": "text-body-2"},
                      "text": f"正在探测 {self._host_of(discovery.get('base_url') or '')} 的模型目录"},
                     {"component": "span", "props": {"class": "text-caption text-medium-emphasis"},
                      "text": f"{finished}/{total} 个协议"},
                 ]},
                {"component": "VProgressLinear",
                 "props": {"model-value": round(finished * 100 / total), "height": 6, "rounded": True,
                           "color": "success"}},
            ]})

        if not rows:
            return None

        rows.append({"component": "div",
                     "props": {"class": "d-flex flex-wrap align-center px-3 py-2", "style": "gap: 6px;"},
                     "content": [
                         self._api_btn("刷新进度", "noop", {}, color="primary", variant="tonal", size="small"),
                         {"component": "span", "props": {"class": "text-caption text-medium-emphasis"},
                          "text": "任务在后台执行；点击“刷新进度”或打开下方仪表盘可自动刷新"
                                  if busy else "任务已完成"},
                     ]})

        return {
            "component": "VCard",
            "props": {"variant": "tonal", "class": "mb-4"},
            "content": [
                {"component": "VCardTitle", "props": {"class": "text-subtitle-1"}, "text": "任务进度"},
                {"component": "VCardText", "props": {"class": "pa-0 pb-2"}, "content": rows},
            ],
        }

    def get_page(self) -> List[dict]:
        """详情页：状态总览、统计卡片、模板卡片、探测结果与运行日志。"""
        current = self._current_llm_config()
        profiles = self._load_profiles()
        runtime = self._runtime or {}
        config = self.get_config() or {}
        last_action = str(config.get("last_action_message") or "").strip()
        last_action_at = str(config.get("last_action_at") or "").strip()

        last_ok = runtime.get("last_check_ok")
        if last_ok is None:
            check_color, check_text = "secondary", "尚未探活"
        elif last_ok:
            check_color = "success"
            check_text = f"探活正常 · {runtime.get('last_check_duration') or 0}ms"
        else:
            check_color = "error"
            check_text = "探活失败"
        fail_count = self._to_int(runtime.get("fail_count"), 0)
        healthy = sum(1 for p in profiles if (p.get("health") or {}).get("status") == "ok")
        failed = sum(1 for p in profiles if (p.get("health") or {}).get("status") == "fail")
        total = len(profiles)
        unknown = max(0, total - healthy - failed)
        health_ratio = round(healthy * 100 / total) if total else 0

        provider_label = self._provider_label(current.get("LLM_PROVIDER"))
        model_name = current.get("LLM_MODEL") or "-"
        endpoint_host = self._host_of(current.get("LLM_BASE_URL") or "")
        protocol = current.get("LLM_API_PROTOCOL") or "auto"
        thinking = current.get("LLM_THINKING_LEVEL") or "off"
        has_key = bool(current.get("LLM_API_KEY"))

        def chip(text: Any, color: str = "", variant: str = "tonal", size: str = "small", **props) -> dict:
            item_props: Dict[str, Any] = {
                "size": size, "variant": variant, "class": "me-2 mb-1", **props,
            }
            if color:
                item_props["color"] = color
            return {"component": "VChip", "props": item_props, "text": str(text)}

        def stat_tile(label: str, value: Any, color: str, hint: str = "") -> dict:
            content = [
                {"component": "div",
                 "props": {"class": f"text-h5 font-weight-bold text-{color}"}, "text": str(value)},
                {"component": "div",
                 "props": {"class": "text-body-2 font-weight-medium"}, "text": label},
            ]
            if hint:
                content.append({"component": "div",
                                "props": {"class": "text-caption text-medium-emphasis"}, "text": hint})
            return {
                "component": "VCol", "props": {"cols": 6, "md": 3},
                "content": [{"component": "VCard", "props": {"variant": "tonal", "class": "h-100"},
                             "content": [{"component": "VCardText",
                                          "props": {"class": "py-3 text-center"}, "content": content}]}],
            }

        def template_card(profile: Dict[str, Any], index: int) -> dict:
            llm = self._effective_llm(self._profile_llm(profile))
            health = profile.get("health") or {}
            active = self._profile_is_active(profile, current)
            status = health.get("status") or "unknown"
            if status == "ok":
                health_color = "success"
                health_text = f"正常 · {health.get('duration_ms') or 0}ms"
            elif status == "fail":
                health_color = "error"
                health_text = "失败"
            else:
                health_color = "secondary"
                health_text = "未探活"
            top_chips = []
            if active:
                top_chips.append(chip("生效中", "success", "flat", size="x-small"))
            if not llm.get("LLM_API_KEY"):
                top_chips.append(chip("无密钥", "warning", "outlined", size="x-small"))
            top_chips.append(chip(health_text, health_color, "tonal", size="x-small",
                                  title=(health.get("message") or "")[:120]))
            top_chips.append(chip(self._provider_label(llm.get("LLM_PROVIDER")), "", "tonal", size="x-small"))
            actions = [
                self._api_btn("应用", "apply", {"pid": profile.get("id")},
                              color="secondary" if active else "primary", variant="tonal", size="small"),
                self._api_btn("探活", "probe", {"pid": profile.get("id")},
                              color="info", variant="tonal", size="small"),
                self._api_btn("↑", "move", {"pid": profile.get("id"), "dir": "up"},
                              color="default", variant="text", size="small"),
                self._api_btn("↓", "move", {"pid": profile.get("id"), "dir": "down"},
                              color="default", variant="text", size="small"),
                self._api_btn("更新", "save_current", {"name": profile.get("name")}, color="warning",
                              variant="text", size="small",
                              title="用当前系统配置覆盖该模板快照（与「应用」方向相反）"),
                self._api_btn("删除", "delete", {"pid": profile.get("id")},
                              color="error", variant="text", size="small"),
            ]
            return {
                "component": "VCard",
                "props": {"variant": "outlined", "class": "ma-3"},
                "content": [{"component": "VCardText", "props": {"class": "pb-1"}, "content": [
                    {"component": "div",
                     "props": {"class": "d-flex flex-wrap align-center", "style": "gap: 4px;"},
                     "content": [
                         {"component": "span", "props": {"class": "text-caption text-medium-emphasis me-1"},
                          "text": f"#{index + 1}"},
                         {"component": "span", "props": {"class": "text-subtitle-1 font-weight-medium me-2"},
                          "text": profile.get("name") or "-"},
                     ] + top_chips},
                    {"component": "VRow", "props": {"dense": True, "class": "mt-1"}, "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 8}, "content": [
                            {"component": "div", "props": {"class": "text-body-2"},
                             "text": f"{llm.get('LLM_MODEL') or '-'}"},
                            {"component": "div",
                             "props": {"class": "text-caption text-medium-emphasis"},
                             "text": f"{self._host_of(llm.get('LLM_BASE_URL') or '')} · "
                                     f"{llm.get('LLM_API_PROTOCOL') or 'auto'}"},
                            {"component": "div",
                             "props": {"class": "text-caption text-medium-emphasis"},
                             "text": f"最近探活：{health.get('checked_at') or '未探活'}"
                                     f"{(' · ' + (health.get('message') or '')[:60]) if health.get('message') else ''}"},
                        ]},
                        {"component": "VCol",
                         "props": {"cols": 12, "md": 4, "class": "d-flex flex-wrap align-center justify-end",
                                   "style": "gap: 2px;"},
                         "content": actions},
                    ]},
                ]}],
            }

        def banner(text: str, level: str = "info") -> dict:
            return {"component": "VAlert",
                    "props": {"type": level, "variant": "tonal", "class": "mb-4", "density": "compact",
                              "text": text}}

        status_chips = [
            chip(f"{provider_label} / {model_name}", "primary", "flat"),
            chip(f"端点 {endpoint_host}"),
            chip("密钥已配置" if has_key else "密钥未配置", "success" if has_key else "warning"),
            chip(f"思考 {thinking}"),
            chip(check_text, check_color, "flat"),
        ]
        runtime_chips = [
            chip(f"连续失败 {fail_count}/{self._fail_threshold}", "error" if fail_count else "secondary"),
            chip(f"自动切换{'开启' if self._auto_failover else '关闭'}",
                 "success" if self._auto_failover else "secondary"),
            chip(f"即时接管{'开启' if self._takeover else '关闭'}",
                 "success" if self._takeover else "secondary"),
            chip(f"全局参数{'开启' if self._apply_global else '关闭'}",
                 "primary" if self._apply_global else "secondary"),
        ]
        if runtime.get("last_check_at"):
            runtime_chips.append(chip(f"最近检查 {runtime.get('last_check_at')}"))

        toolbar = [
            self._api_btn("保存当前配置为模板", "save_current", {},
                          color="primary", variant="tonal", size="small"),
            self._api_btn("探活当前配置", "probe", {}, color="info", variant="tonal", size="small"),
            self._api_btn("探活全部模板", "probe_all", {}, color="info", variant="tonal", size="small"),
            self._api_btn("立即执行切换检查", "failover_now", {},
                          color="warning", variant="tonal", size="small"),
            self._api_btn("探测端点模型", "discover", {}, color="success", variant="tonal", size="small"),
            self._api_btn("应用全局参数", "apply_global", {}, color="secondary", variant="tonal", size="small"),
            self._api_btn("刷新数据", "noop", {}, color="default", variant="tonal", size="small"),
        ]

        hero = {
            "component": "VCard",
            "props": {"variant": "tonal", "class": "mb-4"},
            "content": [
                {"component": "VCardTitle",
                 "props": {"class": "d-flex flex-wrap align-center justify-space-between"},
                 "content": [
                     {"component": "div", "props": {"class": "d-flex flex-wrap align-center"},
                      "content": [
                          {"component": "span", "props": {"class": "text-h6 me-2"}, "text": "API 聚合自动切换"},
                          chip(f"v{self.plugin_version}", "primary", "flat", size="x-small"),
                          chip("已启用" if self._enabled else "已停用",
                               "success" if self._enabled else "secondary", size="x-small"),
                      ]},
                     {"component": "div", "props": {"class": "d-flex flex-wrap align-center",
                                                    "style": "gap: 4px;"},
                      "content": [
                          self._api_btn("保存当前配置", "save_current", {}, color="primary", variant="flat"),
                          self._api_btn("立即检查", "failover_now", {}, color="warning", variant="tonal"),
                          self._api_btn("刷新", "noop", {}, color="default", variant="text"),
                      ]},
                 ]},
                {"component": "VCardText", "props": {"class": "pt-1"}, "content": [
                    {"component": "VRow", "props": {"dense": True}, "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 7}, "content": [
                            {"component": "div", "props": {"class": "text-h5 font-weight-medium"},
                             "text": f"{provider_label} / {model_name}"},
                            {"component": "div",
                             "props": {"class": "text-caption text-medium-emphasis mb-2"},
                             "text": f"{endpoint_host} · {protocol} · 模板顺序即切换优先级"},
                            {"component": "div", "props": {"class": "d-flex flex-wrap align-center"},
                             "content": status_chips},
                        ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 5}, "content": [
                            {"component": "div", "props": {"class": "text-caption text-medium-emphasis mb-1"},
                             "text": "运行状态"},
                            {"component": "div", "props": {"class": "d-flex flex-wrap align-center"},
                             "content": runtime_chips},
                        ]},
                    ]},
                    {"component": "VDivider", "props": {"class": "my-3"}},
                    {"component": "div", "props": {"class": "d-flex flex-wrap align-center",
                                                    "style": "gap: 6px;"},
                     "content": toolbar},
                ]},
            ],
        }

        stats = {
            "component": "VRow",
            "props": {"class": "mb-2"},
            "content": [
                stat_tile("模板总数", total, "primary", "顺序即切换优先级"),
                stat_tile("健康模板", healthy, "success", f"覆盖率 {health_ratio}%"),
                stat_tile("失败模板", failed, "error", "最近一次探活"),
                stat_tile("未探活", unknown, "secondary", "建议执行全量探活"),
            ],
        }
        health_card = {
            "component": "VCard",
            "props": {"variant": "outlined", "class": "mb-4"},
            "content": [{"component": "VCardText", "props": {"class": "py-3"}, "content": [
                {"component": "div", "props": {"class": "d-flex align-center justify-space-between mb-1"},
                 "content": [
                     {"component": "span", "props": {"class": "text-caption text-medium-emphasis"},
                      "text": f"模板健康覆盖 {healthy}/{total}"},
                     {"component": "span", "props": {"class": "text-caption text-medium-emphasis"},
                      "text": f"最近检查 {runtime.get('last_check_at') or '尚未检查'}"},
                 ]},
                {"component": "VProgressLinear",
                 "props": {"model-value": health_ratio, "height": 8, "rounded": True, "color": "success"}},
            ]}],
        }

        total_pages = self._total_pages(total)
        page = self._current_page(total)
        start = (page - 1) * self._PAGE_SIZE
        page_profiles = profiles[start:start + self._PAGE_SIZE]
        template_content: List[dict] = [template_card(profile, start + offset)
                                        for offset, profile in enumerate(page_profiles)]
        if not profiles:
            template_content = [{"component": "VAlert",
                                 "props": {"type": "info", "variant": "tonal", "class": "ma-3",
                                           "density": "compact",
                                           "text": "还没有模板。点击上方“保存当前配置为模板”即可创建第一个模板。"}}]
        elif total_pages > 1:
            template_content.append({
                "component": "div",
                "props": {"class": "d-flex flex-wrap align-center justify-center px-3 py-3 border-t",
                          "style": "gap: 4px;"},
                "content": [
                    self._api_btn("«", "set_page", {"p": 1}, color="default", variant="tonal", size="small"),
                    self._api_btn("上一页", "set_page", {"p": max(1, page - 1)},
                                  color="primary", variant="tonal", size="small"),
                    {"component": "span", "props": {"class": "text-caption text-medium-emphasis mx-1"},
                     "text": f"{page}/{total_pages}"},
                    self._api_btn("下一页", "set_page", {"p": min(total_pages, page + 1)},
                                  color="primary", variant="tonal", size="small"),
                    self._api_btn("»", "set_page", {"p": total_pages},
                                  color="default", variant="tonal", size="small"),
                ],
            })
        templates_card = {
            "component": "VCard",
            "props": {"class": "mb-4"},
            "content": [
                {"component": "VCardTitle",
                 "props": {"class": "d-flex flex-wrap align-center justify-space-between"},
                 "content": [
                     {"component": "div", "props": {"class": "d-flex flex-wrap align-center"}, "content": [
                         {"component": "span", "props": {"class": "text-subtitle-1 me-2"},
                          "text": f"配置模板（{total}）"},
                         chip("顺序即故障切换优先级", "", "tonal", size="x-small"),
                         *([chip(f"第 {page}/{total_pages} 页", "primary", "tonal", size="x-small")]
                           if total_pages > 1 else []),
                     ]},
                     {"component": "div", "props": {"class": "d-flex flex-wrap align-center",
                                                    "style": "gap: 2px;"},
                      "content": [
                          self._api_btn("规范命名", "normalize_names", {"scope": "discover"},
                                        color="default", variant="text", size="small"),
                          self._api_btn("清理名单外", "prune_offlist", {},
                                        color="warning", variant="text", size="small"),
                      ]},
                 ]},
                {"component": "VCardText", "props": {"class": "pa-0"}, "content": template_content},
            ],
        }

        discovery = self._load_discovery()
        discovery_card = self._discovery_card(discovery)

        log_items = []
        for item in (runtime.get("log") or [])[:12]:
            level = item.get("level") or "info"
            color = {"error": "error", "warn": "warning"}.get(level, "info")
            log_items.append({
                "component": "div",
                "props": {"class": "d-flex align-start px-3 py-1 border-t text-caption"},
                "content": [
                    chip(item.get("time") or "-", color, "tonal", size="x-small"),
                    {"component": "div", "props": {"class": "pt-1"}, "text": item.get("text") or ""},
                ],
            })
        if not log_items:
            log_items = [{"component": "div",
                          "props": {"class": "px-3 py-3 text-caption text-medium-emphasis"},
                          "text": "暂无切换与探活记录"}]
        logs_card = {
            "component": "VCard",
            "content": [
                {"component": "VCardTitle",
                 "props": {"class": "d-flex align-center justify-space-between"},
                 "content": [
                     {"component": "span", "props": {"class": "text-subtitle-1"}, "text": "切换与探活记录"},
                     self._api_btn("清空", "clear_log", {}, color="error", variant="text", size="x-small"),
                 ]},
                {"component": "VCardText", "props": {"class": "pa-0 pb-2"}, "content": log_items},
            ],
        }

        cards: List[dict] = []
        progress_card = self._progress_card()
        if progress_card:
            cards.append(progress_card)
        if not self._enabled:
            cards.append(banner("插件当前未启用：定时探活与自动切换不会运行。", "warning"))
        if last_action:
            level = "error" if any(word in last_action for word in ("失败", "错误", "拒绝")) else "info"
            cards.append(banner(f"{last_action_at + ' · ' if last_action_at else ''}{last_action}", level))
        if not self._include_credentials:
            cards.append(banner(
                "模板不保存 API Key：跨供应商或跨端点切换会被拒绝，避免把当前密钥发往其他端点。", "warning"))
        cards.extend([hero, stats, health_card, templates_card, discovery_card, logs_card])
        return cards
    # ------------------------------------------------------------------
    # 仪表盘（支持自动刷新）
    # ------------------------------------------------------------------

    def get_dashboard_meta(self) -> Optional[List[Dict[str, str]]]:
        """声明一个可自动刷新的进度仪表盘。"""
        if not self.get_state():
            return []
        return [{"key": "progress", "name": f"{self.plugin_name} · 任务进度"}]

    def get_dashboard(self, key: str = "", **kwargs) -> Optional[Tuple[Dict[str, Any], Dict[str, Any],
                                                                      Optional[List[dict]]]]:
        """
        自动刷新的进度面板。

        详情页的 Vuetify 配置不支持自行轮询，这里用仪表盘的 refresh 能力做到
        探测/探活过程中每 5 秒自动刷新一次。
        """
        runtime = self._runtime or {}
        discovery = self._load_discovery()
        probe = runtime.get("probe_all") or {}
        current = self._current_llm_config()

        col = {"cols": 12, "md": 6}
        # 忙时 5 秒刷新跟踪进度；空闲时降到 30 秒，减少无谓轮询
        busy = (probe.get("status") == "running" or discovery.get("status") == "running")
        global_config = {"refresh": 5 if busy else 30, "border": True,
                         "title": self.plugin_name, "subtitle": "探测与探活进度"}

        items: List[dict] = [
            {"component": "div", "props": {"class": "d-flex flex-wrap align-center mb-2"}, "content": [
                {"component": "VChip",
                 "props": {"size": "small", "color": "primary", "variant": "flat", "class": "me-2 mb-1"},
                 "text": f"{current.get('LLM_PROVIDER') or '-'} / {current.get('LLM_MODEL') or '-'}"},
                {"component": "VChip", "props": {"size": "small", "variant": "tonal", "class": "me-2 mb-1"},
                 "text": self._host_of(current.get("LLM_BASE_URL") or "")},
            ]},
        ]

        if probe.get("status") == "running":
            total = max(1, self._to_int(probe.get("total"), 1))
            done = self._to_int(probe.get("done"), 0)
            items.append({"component": "div", "props": {"class": "text-body-2 mb-1"},
                          "text": f"探活中 {done}/{total}：{probe.get('current') or ''}"})
            items.append({"component": "VProgressLinear",
                          "props": {"model-value": round(done * 100 / total), "height": 6,
                                    "rounded": True, "color": "info", "class": "mb-2"}})
        if discovery.get("status") == "running":
            total = max(1, len(self._discover_providers))
            done = len(discovery.get("providers") or {})
            items.append({"component": "div", "props": {"class": "text-body-2 mb-1"},
                          "text": f"模型探测中 {done}/{total} 个协议："
                                  f"{self._host_of(discovery.get('base_url') or '')}"})
            items.append({"component": "VProgressLinear",
                          "props": {"model-value": round(done * 100 / total), "height": 6,
                                    "rounded": True, "color": "success", "class": "mb-2"}})

        if probe.get("status") != "running" and discovery.get("status") != "running":
            last_ok = runtime.get("last_check_ok")
            if last_ok is None:
                text = "空闲｜尚未探活"
            elif last_ok:
                text = f"空闲｜当前配置正常 {runtime.get('last_check_duration') or 0}ms" \
                       f"（{runtime.get('last_check_at') or '-'}）"
            else:
                text = f"空闲｜当前配置探活失败：{str(runtime.get('last_check_message') or '')[:60]}"
            items.append({"component": "div", "props": {"class": "text-body-2"}, "text": text})
            if probe.get("finished_at"):
                items.append({"component": "div", "props": {"class": "text-caption text-medium-emphasis"},
                              "text": f"上次全量探活 {probe.get('finished_at')}：正常 "
                                      f"{self._to_int(probe.get('ok'), 0)} 个，失败 "
                                      f"{self._to_int(probe.get('fail'), 0)} 个"})

        return col, global_config, [{"component": "div", "props": {"class": "pa-3"}, "content": items}]
