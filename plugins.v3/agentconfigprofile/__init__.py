"""
MP Switch 插件入口：宿主生命周期、兼容迁移、API 与事件注册。
"""

import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.schemas.response import Response
from app.schemas.types import ChainEventType
from app.sdk.config import settings
from app.sdk.events import Event, eventmanager
from app.sdk.logging import logger
from app.sdk.plugin.base import _PluginBase
from apscheduler.triggers.cron import CronTrigger

from .discovery import DiscoveryMixin
from .failover import FailoverMixin
from .health import HealthMixin
from .modes import ModeMixin
from .profiles import ProfileMixin
from .views import ViewMixin


class agentconfigprofile(ModeMixin, ProfileMixin, HealthMixin, DiscoveryMixin, FailoverMixin, ViewMixin, _PluginBase):
    """
    MP Switch：智能助手供应商配置管理与故障切换。

    将当前智能助手（Agent）的 LLM 供应商配置保存为命名模板，支持一键切换；
    并可定时探活当前生效配置，失效时按模板顺序自动切换到可用配置。
    采用 Vuetify（免构建）渲染模式，详情页即操作台。

    注意：插件 ID 恒等于类名，这里刻意使用全小写类名，与插件市场安装统计
    服务端已有的 `agentconfigprofile` 记录保持一致，否则市场卡片读不到下载量。
    旧版大写 ID（AgentConfigProfile）的配置与数据会在首次加载时自动迁移。
    """

    plugin_name = "MP Switch"
    plugin_desc = "参考CC Switch制作，仓库地址https://github.com/farion1231/cc-switch?tab=readme-ov-file"
    plugin_icon = "agentresourceofficer.png"
    plugin_version = "3.0.0"
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

    @property
    def _settings(self):
        """
        为各职责模块提供同一宿主设置入口。
        """
        return settings

    def __init__(self) -> None:
        """
        初始化当前插件实例的锁、任务代数和运行状态。
        """
        super().__init__()
        self._lock = threading.RLock()
        self._failover_lock = threading.Lock()
        self._apply_lock = threading.RLock()
        self._stop_event = threading.Event()
        self._circuits = {}
        self._generation = 0
        self._runtime: Dict[str, Any] = {}
        self._takeover_pending = False

    def init_plugin(self, config: dict = None):
        """
        初始化插件配置，并处理来自配置页的一次性动作。
        """
        # v3 initialize() 会在同一个实例上再次调用 init_plugin()。先停止旧代后台
        # 任务，并保留同一把锁，避免旧线程与新配置各自使用不同锁而发生交叉写入。
        self._stop_mode_gateway()
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
        self._circuits = {}
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
        self._discovery_base_url_runtime = ""

        self._runtime = self._load_runtime()
        self._init_modes()
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
        """
        后台执行安装统计上报，避免阻塞插件加载。
        """
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


    def _save_persistent_config(self, action_message: str = ""):
        """
        持久化配置并清空一次性动作字段，避免重复执行。
        """
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
        """
        返回插件启用状态，供宿主展示与调度使用。
        """
        return bool(getattr(self, "_enabled", False))

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        本插件不注册聊天命令。
        """
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册定时探活服务。
        """
        if (not self._enabled or not self._auto_failover or not self._check_cron
                or self._mode_active() == "stack"
                or (self._mode_active() == "direct" and self._mode_state().get("explicit_mode"))):
            return []
        return [{
            "id": "agentconfigprofile_health_check",
            "name": "智能助手配置探活与自动切换",
            "trigger": CronTrigger.from_crontab(self._check_cron),
            "func": self.health_check_job,
            "kwargs": {},
        }]

    def get_api(self) -> List[Dict[str, Any]]:
        """
        声明供宿主数据详情页调用的受鉴权保护的操作接口。
        """
        return [
            *self._mode_apis(),
            {"path": "/save_current", "endpoint": self.api_save_current, "methods": ["GET", "POST"],
             "summary": "保存当前配置为模板"},
            {"path": "/toggle_queue", "endpoint": self.api_toggle_queue, "methods": ["GET", "POST"],
             "summary": "切换配置是否参加自动故障转移"},
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
        """
        插件停用或重载时通知后台线程退出，避免旧实例覆盖新实例的数据。
        """
        self._stop_mode_gateway()
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
        """
        校验 cron 表达式，无效时回退默认值。
        """
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
        """
        判断后台任务是否属于已停止或已被新配置替换的实例代次。
        """
        event = stop_event if stop_event is not None else getattr(self, "_stop_event", None)
        if event is not None and event.is_set():
            return True
        return generation is not None and generation != getattr(self, "_generation", generation)

    @classmethod
    def _provider_spec(cls, provider: Any):
        """
        按 provider id 获取系统内置的供应商定义，取不到返回 None。
        """
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
        """
        返回供应商展示名，优先用系统定义，其次用探测协议名，最后用原始 id。
        """
        pid = str(provider or "").strip()
        if not pid:
            return "-"
        spec = cls._provider_spec(pid)
        name = getattr(spec, "name", "") if spec else ""
        return name or cls._DISCOVER_PROVIDERS.get(pid, pid)

    @classmethod
    def _provider_needs_api_key(cls, provider: Any) -> bool:
        """
        判断该供应商是否必须配置 API Key（OAuth 类供应商可不填）。
        """
        spec = cls._provider_spec(provider)
        if spec is None:
            return str(provider or "") not in {"chatgpt", "github-copilot"}
        if getattr(spec, "oauth_methods", ()):
            return False
        return bool(getattr(spec, "supports_api_key", True))


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
        """
        记录一条切换/探活日志。
        """
        with self._lock:
            logs = self._runtime.setdefault("log", [])
            logs.insert(0, {"time": self._now(), "text": self._sanitize(text), "level": level})
            del logs[self._LOG_MAX:]


    @eventmanager.register(ChainEventType.AgentLLMProvider, priority=60)
    def select_llm_provider(self, event: Event):
        """
        当前配置已被判定失效时，用备用模板即时接管本次 Agent 调用。
        """
        if self._mode_active() != "direct" or self._mode_state().get("explicit_mode"):
            return
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


    @staticmethod
    def _auth(apikey: str) -> bool:
        return apikey == settings.API_TOKEN

    def _api_run(self, apikey: str, func, *args, **kwargs) -> Response:
        """
        统一处理 API 鉴权与异常，避免任何单点异常让详情页整体报错。
        """
        if not self._auth(apikey):
            return Response(success=False, message="认证失败")
        try:
            message = func(*args, **kwargs)
        except Exception as err:  # noqa: BLE001
            logger.error(f"{self.plugin_name}：接口执行失败 - {err}")
            return Response(success=False, message=f"操作失败：{self._sanitize(err)[:150]}")
        return Response(success=True, message=str(message or "已完成"))

    def api_save_current(self, apikey: str, name: str = ""):
        """
        验证访问令牌后将当前系统配置保存为模板。
        """
        return self._api_run(apikey, self._do_save, str(name or "").strip())

    def api_toggle_queue(self, pid: str, apikey: str):
        """
        将供应商配置加入或移出自动切换队列。
        """
        return self._api_run(apikey, self._toggle_queue, pid)

    def api_apply(self, pid: str, apikey: str):
        """
        验证访问令牌后手动应用指定配置。
        """
        return self._api_run(apikey, self._do_apply, pid, "手动切换")

    def api_probe(self, apikey: str, pid: str = ""):
        """
        验证访问令牌后探活指定配置或当前系统配置。
        """
        def run() -> str:
            if pid:
                _ok, text = self._probe_profile(pid)
                return text
            ok, message, duration = self._probe_current()
            return f"当前配置探活{'成功' if ok else '失败'}：{message}（{duration}ms）"

        return self._api_run(apikey, run)

    def api_probe_all(self, apikey: str):
        """
        验证访问令牌后启动全部配置探活任务。
        """
        return self._api_run(apikey, self._start_probe_all)

    def api_delete(self, pid: str, apikey: str):
        """
        验证访问令牌后删除指定配置。
        """
        return self._api_run(apikey, self._do_delete, pid)

    def api_move(self, pid: str, dir: str, apikey: str):
        """
        验证访问令牌后按给定方向调整配置顺序。
        """
        return self._api_run(apikey, self._do_move, pid, dir)

    def api_set_page(self, apikey: str, p: str = "1"):
        """
        切换模板列表分页。
        """
        return self._api_run(apikey, self._set_page, p)

    def api_failover_now(self, apikey: str):
        """
        验证访问令牌并确认插件启用后立即执行故障切换检查。
        """
        def run() -> str:
            with self._lock:
                self._runtime["failover_block_until"] = ""
            return self.health_check_job()

        return self._api_run(apikey, run)

    def api_clear_log(self, apikey: str):
        """
        验证访问令牌后清空本插件的切换记录。
        """
        def run() -> str:
            with self._lock:
                self._runtime["log"] = []
                self._save_runtime()
            return "日志已清空"

        return self._api_run(apikey, run)

    def api_apply_global(self, apikey: str):
        """
        验证访问令牌后把全局参数同步到配置模板与系统设置。
        """
        return self._api_run(apikey, self._apply_global_options)

    def api_noop(self, apikey: str):
        """
        不做任何变更，仅让前端重新拉取页面数据。
        """
        if not self._auth(apikey):
            return Response(success=False, message="认证失败")
        progress = (self._runtime or {}).get("probe_all") or {}
        discovery = self._load_discovery()
        if progress.get("status") == "running":
            return Response(
                success=True,
                message=f"探活进行中 {self._to_int(progress.get('done'), 0)}/"
                        f"{self._to_int(progress.get('total'), 0)}：{progress.get('current') or ''}")
        if discovery.get("status") == "running":
            return Response(success=True,
                                    message=f"模型探测进行中，已完成 {len(discovery.get('providers') or {})} 个协议")
        return Response(success=True, message="已刷新")

    def api_discover(self, apikey: str, base_url: str = "", api_key: str = "", pid: str = ""):
        """
        验证访问令牌后启动指定端点的模型发现。
        """
        return self._api_run(apikey, self._start_discovery, base_url, api_key, pid)

    def api_import_discovered(self, apikey: str, provider: str = ""):
        """
        验证访问令牌后将已发现的模型导入为配置模板。
        """
        return self._api_run(apikey, self._import_discovered, provider)

    def api_add_model(self, provider: str, model: str, apikey: str):
        """
        验证访问令牌后将单个已发现模型加入模板。
        """
        return self._api_run(apikey, self._add_model_profile, provider, model)

    def api_clear_discovery(self, apikey: str):
        """
        验证访问令牌后清空模型发现结果。
        """
        def run() -> str:
            self._save_discovery({})
            return "探测结果已清除"

        return self._api_run(apikey, run)

    def api_normalize_names(self, apikey: str, scope: str = "discover"):
        """
        验证访问令牌后规范指定范围内的模板名称。
        """
        return self._api_run(apikey, self._normalize_names, scope)

    def api_prune_offlist(self, apikey: str):
        """
        验证访问令牌后清理白名单之外的自动探测模板。
        """
        return self._api_run(apikey, self._prune_offlist)
