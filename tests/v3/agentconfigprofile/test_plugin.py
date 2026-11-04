"""
agentconfigprofile 插件回归测试。

覆盖旧 ID 迁移与重置、同模型多密钥指纹、应用配置串行化、接管时效与密钥边界、
探测取消终态、页面密钥保护，以及 MP Switch 重构后的插件身份和发布元信息。
"""

from __future__ import annotations

import json
import threading
import time
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import agentconfigprofile as mod
import pytest
from agentconfigprofile import agentconfigprofile

PLUGIN_ID = "agentconfigprofile"
LEGACY_ID = "AgentConfigProfile"


class FakeSettings:
    """
    仅提供插件会读取的设置字段和 update_settings 合同。
    """

    API_TOKEN = "test-token"
    LLM_PROVIDER = "openai"
    LLM_MODEL = "gpt-current"
    LLM_API_KEY = "sk-current"
    LLM_BASE_URL = "https://current.example/v1"
    LLM_BASE_URL_PRESET = None
    LLM_USER_AGENT = "test-agent"
    LLM_USE_PROXY = True
    LLM_THINKING_LEVEL = "off"
    LLM_API_PROTOCOL = "auto"
    LLM_WEB_SEARCH_MODE = "local"
    LLM_MAX_CONTEXT_TOKENS = 128
    LLM_TEMPERATURE = 0.3
    LLM_SUPPORT_IMAGE_INPUT = False
    LLM_SUPPORT_AUDIO_INPUT = False
    LLM_SUPPORT_AUDIO_OUTPUT = False
    AI_AGENT_RETRY_TRANSFER = False
    AI_RECOMMEND_ENABLED = False
    AI_AGENT_JOB_INTERVAL = 0
    PLUGIN_STATISTIC_SHARE = False

    def __init__(self) -> None:
        """
        初始化设置写入记录和模拟锁定字段。
        """
        self.locked: set[str] = set()
        self.writes: list[dict] = []

    def update_settings(self, env: dict) -> dict:
        """
        写入内存设置并返回各字段是否被锁定。

        :param env: 需要模拟写入的设置字段
        :return: 字段名到写入状态及原因的映射
        """
        self.writes.append(dict(env))
        results = {}
        for key, value in env.items():
            if key in self.locked:
                results[key] = (False, "locked")
                continue
            setattr(self, key, value)
            results[key] = (True, "")
        return results


class FakeStore:
    """
    用内存字典模拟宿主插件配置与 PluginData。
    """

    def __init__(self) -> None:
        """
        创建与真实宿主数据隔离的插件配置和数据容器。
        """
        self.configs: dict[str, dict] = {}
        self.data: dict[tuple[str, str], object] = {}

    def get_config(self, plugin_id: str | None = None, **_kwargs) -> dict | None:
        """
        返回指定插件配置的副本，防止测试直接修改存储对象。

        :param plugin_id: 插件标识；省略时使用当前插件
        :return: 配置副本；未保存时返回 None
        """
        return deepcopy(self.configs.get(plugin_id or PLUGIN_ID))

    def update_config(self, config: dict, plugin_id: str | None = None, **_kwargs) -> bool:
        """
        把指定插件配置的副本保存到内存。

        :param config: 要保存的配置
        :param plugin_id: 插件标识；省略时使用当前插件
        :return: 写入成功标记
        """
        self.configs[plugin_id or PLUGIN_ID] = deepcopy(config)
        return True

    def get_data(self, key: str | None = None, plugin_id: str | None = None, **_kwargs):
        """
        返回内存插件数据的副本。

        :param key: 数据键
        :param plugin_id: 插件标识；省略时使用当前插件
        :return: 已保存的数据副本；不存在时返回 None
        """
        return deepcopy(self.data.get((plugin_id or PLUGIN_ID, key or "")))

    def save_data(self, key: str, value, plugin_id: str | None = None, **_kwargs) -> None:
        """
        将数据副本保存到指定插件的内存空间。

        :param key: 数据键
        :param value: 要保存的数据
        :param plugin_id: 插件标识；省略时使用当前插件
        """
        self.data[(plugin_id or PLUGIN_ID, key)] = deepcopy(value)


def llm(provider="openai", model="gpt-a", base_url="https://a.example/v1",
        key="sk-a", protocol="auto", preset=None) -> dict:
    return {
        "LLM_PROVIDER": provider,
        "LLM_MODEL": model,
        "LLM_BASE_URL": base_url,
        "LLM_BASE_URL_PRESET": preset,
        "LLM_API_PROTOCOL": protocol,
        "LLM_API_KEY": key,
        "LLM_USER_AGENT": "ua",
        "LLM_USE_PROXY": True,
        "LLM_THINKING_LEVEL": "off",
        "LLM_WEB_SEARCH_MODE": "local",
        "LLM_MAX_CONTEXT_TOKENS": 128,
        "LLM_TEMPERATURE": 0.3,
    }


def profile(pid="p1", name="模板一", **llm_kwargs) -> dict:
    return {
        "id": pid,
        "name": name,
        "snapshot": {"llm": llm(**llm_kwargs)},
        "health": {"status": "unknown", "fail_count": 0},
        "created_at": "2026-01-01 00:00:00",
        "updated_at": "2026-01-01 00:00:00",
    }


@pytest.fixture()
def plugin(monkeypatch):
    settings = FakeSettings()
    monkeypatch.setattr(mod, "settings", settings)
    store = FakeStore()
    p = object.__new__(agentconfigprofile)
    p._lock = threading.RLock()
    p._failover_lock = threading.Lock()
    p._apply_lock = threading.RLock()
    p._stop_event = threading.Event()
    p._generation = 1
    p._runtime = {
        "fail_count": 0,
        "fail_endpoint": "",
        "failover_block_until": "",
        "log": [],
        "page": 1,
        "takeover_id": "",
    }
    p._enabled = True
    p._include_credentials = True
    p._notify = False
    p._auto_failover = False
    p._auto_recover = False
    p._takeover = True
    p._check_cron = "*/30 * * * *"
    p._fail_threshold = 2
    p._test_timeout = 5
    p._probe_prompt = "请只回复 OK"
    p._discover_base_url = "https://a.example/v1"
    p._discover_api_key = ""
    p._discover_providers = ["openai"]
    p._discover_filter = ""
    p._discover_limit = 20
    p._discover_auto_import = False
    p._discover_smart_match = True
    p._whitelist = []
    p._apply_global = False
    p._g_thinking_level = "off"
    p._g_use_proxy = True
    p._g_image_input = False
    p._g_audio_input = False
    p._g_audio_output = False
    p._g_retry_transfer = False
    p._g_ai_recommend = False
    p._g_api_protocol = "auto"
    p._g_web_search_mode = "local"
    p._g_user_agent = ""
    p._g_job_interval = 0
    p._discovery_api_key_runtime = ""
    p._takeover_pending = False
    p.get_config = store.get_config
    p.update_config = store.update_config
    p.get_data = store.get_data
    p.save_data = store.save_data
    p._provider_label = lambda provider: str(provider or "-")
    p._provider_needs_api_key = lambda provider: True
    return p, store, settings


def test_same_endpoint_requires_same_key_and_protocol():
    a = llm(key="sk-a")
    assert agentconfigprofile._same_endpoint(a, llm(key="sk-a"))
    assert not agentconfigprofile._same_endpoint(a, llm(key="sk-b"))
    assert agentconfigprofile._same_endpoint(a, llm(key=""))
    assert not agentconfigprofile._same_endpoint(a, llm(protocol="chat_completions"))


def test_migration_never_overwrites_existing_config(plugin):
    p, store, _ = plugin
    store.configs[PLUGIN_ID] = {"enabled": True, "whitelist": "new"}
    store.configs[LEGACY_ID] = {"enabled": False, "whitelist": "old"}
    store.data[(PLUGIN_ID, "profiles")] = [profile("new")]
    store.data[(LEGACY_ID, "profiles")] = [profile("legacy")]

    result = p._migrate_legacy_id({"enabled": True, "whitelist": "new"})

    assert result["whitelist"] == "new"
    assert store.configs[PLUGIN_ID]["whitelist"] == "new"
    assert [item["id"] for item in store.data[(PLUGIN_ID, "profiles")]] == ["new"]
    assert store.configs[LEGACY_ID][p._LEGACY_MIGRATED_KEY]


def test_migration_backfills_legacy_marker_when_runtime_marker_exists(plugin):
    p, store, _ = plugin
    store.configs[PLUGIN_ID] = {"enabled": True}
    store.configs[LEGACY_ID] = {"enabled": False, "whitelist": "old"}
    store.data[(PLUGIN_ID, "runtime")] = {"legacy_migrated_at": "2026-01-01 00:00:00"}

    result = p._migrate_legacy_id({"enabled": True})

    assert result == {"enabled": True}
    assert store.configs[PLUGIN_ID] == {"enabled": True}
    assert store.configs[LEGACY_ID][p._LEGACY_MIGRATED_KEY]


def test_reset_does_not_resurrect_legacy_config_or_data(plugin):
    p, store, _ = plugin
    store.configs[LEGACY_ID] = {
        "enabled": True,
        "whitelist": "old",
        p._LEGACY_MIGRATED_KEY: "2026-01-01 00:00:00",
    }
    store.data[(LEGACY_ID, "profiles")] = [profile("legacy")]

    result = p._migrate_legacy_id({})

    assert result == {}
    assert not store.data.get((PLUGIN_ID, "profiles"))
    assert PLUGIN_ID not in store.configs


def test_do_save_replaces_snapshot_and_removes_old_key(plugin):
    p, store, _settings = plugin
    p._include_credentials = False
    store.data[(PLUGIN_ID, "profiles")] = [profile("p1", name="模板一")]
    p._save_profiles = lambda items: store.save_data("profiles", items)

    message = p._do_save("模板一")

    assert "已保存" in message
    saved = store.data[(PLUGIN_ID, "profiles")][0]
    assert "LLM_API_KEY" not in saved["snapshot"]["llm"]
    assert saved["health"]["status"] == "unknown"


def test_do_apply_rejects_cross_endpoint_without_key(plugin):
    p, store, _ = plugin
    p._include_credentials = False
    store.data[(PLUGIN_ID, "profiles")] = [
        profile("p1", provider="anthropic", base_url="https://other.example/v1", key="")
    ]
    p._apply_llm_config = Mock(return_value=True)

    with pytest.raises(RuntimeError, match="拒绝"):
        p._do_apply("p1")

    p._apply_llm_config.assert_not_called()


def test_do_apply_serializes_concurrent_writes(plugin):
    p, store, _ = plugin
    store.data[(PLUGIN_ID, "profiles")] = [
        profile("p1", name="A", model="gpt-a", key="sk-a"),
        profile("p2", name="B", model="gpt-b", key="sk-b"),
    ]
    active = 0
    max_active = 0
    gate = threading.Lock()

    def fake_apply(_llm):
        nonlocal active, max_active
        with gate:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.05)
        with gate:
            active -= 1
        return True

    p._apply_llm_config = fake_apply
    threads = [threading.Thread(target=p._do_apply, args=(pid,)) for pid in ("p1", "p2")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)

    assert max_active == 1


def test_healthy_backup_requires_fresh_health_and_key_compatibility(plugin):
    p, store, _ = plugin
    store.data[(PLUGIN_ID, "profiles")] = [
        profile("fresh", name="fresh", model="gpt-b", key="sk-b"),
        profile("stale", name="stale", model="gpt-c", key="sk-c"),
        profile("other", name="other", model="gpt-d", base_url="https://other.example/v1", key=""),
    ]
    fresh = store.data[(PLUGIN_ID, "profiles")][0]
    fresh["health"] = {"status": "ok", "checked_at": datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")}
    stale = store.data[(PLUGIN_ID, "profiles")][1]
    stale["health"] = {
        "status": "ok",
        "checked_at": (datetime.now(timezone.utc).astimezone() - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S"),
    }
    current = p._current_llm_config()

    backup = p._healthy_backup(current)

    assert backup is not None
    assert backup["id"] == "fresh"


def test_probe_refuses_to_send_current_key_to_another_endpoint(plugin, monkeypatch):
    p, _store, _settings = plugin
    p._include_credentials = False
    calls = []

    class FakeLLMHelper:
        @staticmethod
        async def test_current_settings(**kwargs):
            calls.append(kwargs)
            return {"reply_preview": "OK", "duration_ms": 1}

    import app.agent.llm.helper as helper_mod

    monkeypatch.setattr(helper_mod, "LLMHelper", FakeLLMHelper)
    target = llm(provider="anthropic", model="claude", base_url="https://other.example/v1", key="")

    ok, message, _duration = p._probe(target)

    assert not ok
    assert "拒绝复用" in message
    assert calls == []


def test_probe_current_ignores_result_when_config_changes(plugin, monkeypatch):
    p, _store, settings = plugin

    def changing_probe(_llm):
        settings.LLM_MODEL = "gpt-changed"
        return False, "boom", 5

    monkeypatch.setattr(p, "_probe", changing_probe)

    ok, message, _duration = p._probe_current()

    assert not ok
    assert "已变更" in message
    assert p._runtime["fail_count"] == 0


def test_select_llm_provider_requires_fail_state_for_current_fingerprint(plugin):
    p, _store, _settings = plugin
    p._runtime["fail_count"] = 3
    p._runtime["fail_endpoint"] = ""
    event = SimpleNamespace(event_data={"seed": True})

    p.select_llm_provider(event)

    assert not event.event_data.get("selected_provider_id")


def test_select_llm_provider_uses_fresh_backup(plugin):
    p, store, _settings = plugin
    p._runtime["fail_count"] = 3
    p._runtime["fail_endpoint"] = p._fingerprint(p._current_llm_config())
    backup = profile("backup", name="备用", model="gpt-b", key="sk-b")
    backup["health"] = {"status": "ok", "checked_at": datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")}
    store.data[(PLUGIN_ID, "profiles")] = [backup]
    event = SimpleNamespace(event_data={"seed": True})

    p.select_llm_provider(event)

    assert event.event_data["selected_provider_id"] == "backup"
    assert event.event_data["api_key"] == "sk-b"
    assert event.event_data["model"] == "gpt-b"


def test_failover_skips_apply_failure_and_notifies_only_on_success(plugin, monkeypatch):
    p, store, _settings = plugin
    p._notify = True
    p._notify_safe = Mock()
    store.data[(PLUGIN_ID, "profiles")] = [
        profile("bad", name="坏模板", model="gpt-b", key="sk-b"),
        profile("good", name="好模板", model="gpt-c", key="sk-c"),
    ]
    monkeypatch.setattr(p, "_probe_profile", lambda *args, **kwargs: (True, "ok"))

    def fake_apply(profile_id, **_kwargs):
        if profile_id == "bad":
            raise RuntimeError("write failed")
        return "已切换到模板 [好模板]"

    monkeypatch.setattr(p, "_do_apply", fake_apply)

    result = p._do_failover_locked("测试")

    assert result == "已切换到模板 [好模板]"
    p._notify_safe.assert_called_once()
    assert p._notify_safe.call_args.args[0] == "智能助手已自动切换配置"


def test_failover_respects_cooldown(plugin, monkeypatch):
    p, _store, _settings = plugin
    p._runtime["failover_block_until"] = (
        datetime.now(timezone.utc).astimezone() + timedelta(seconds=30)
    ).strftime("%Y-%m-%d %H:%M:%S")
    apply_mock = Mock(return_value="ok")
    monkeypatch.setattr(p, "_do_failover_locked", apply_mock)

    result = p._do_failover("测试")

    assert "冷却期" in result
    apply_mock.assert_not_called()


def test_probe_all_cancel_sets_terminal_state(plugin):
    p, _store, _settings = plugin
    p._runtime["probe_all"] = {
        "status": "running", "total": 1, "done": 0, "ok": 0, "fail": 0, "current": "当前",
    }
    p._stop_event.set()

    p._probe_all_worker(1, p._stop_event)

    assert p._runtime["probe_all"]["status"] == "error"
    assert "停止" in p._runtime["probe_all"]["message"]


def test_import_discovered_refuses_to_create_keyless_templates(plugin):
    p, store, _settings = plugin
    store.data[(PLUGIN_ID, "discovery")] = {
        "status": "done",
        "base_url": "https://a.example/v1",
        "providers": {
            "openai": {
                "ok": True,
                "models": [{"id": "gpt-x", "name": "GPT X"}],
            }
        },
    }
    p._discovery_api_key_runtime = ""

    message = p._import_discovered()

    assert "导入失败" in message
    assert not store.data.get((PLUGIN_ID, "profiles"))


def test_get_page_renders_status_and_never_leaks_api_key(plugin):
    p, store, _settings = plugin
    store.configs[PLUGIN_ID] = {
        "last_action_message": "已保存模板 [测试]",
        "last_action_at": "2026-10-21 12:00:00",
    }
    store.data[(PLUGIN_ID, "profiles")] = [
        profile("p1", name="测试", key="sk-secret"),
    ]

    page = p.get_page()
    rendered = json.dumps(page, ensure_ascii=False)

    assert p.plugin_name in rendered
    assert "供应商配置（1）" in rendered
    assert "已保存模板 [测试]" in rendered
    assert "sk-secret" not in rendered
    assert "test-token" in rendered  # 宿主按钮仍使用 API_TOKEN 鉴权


def test_migration_adopts_legacy_when_current_is_empty(plugin):
    p, store, _ = plugin
    store.configs[LEGACY_ID] = {"enabled": True, "whitelist": "old"}
    store.data[(LEGACY_ID, "profiles")] = [profile("legacy")]

    result = p._migrate_legacy_id({})

    assert result["whitelist"] == "old"
    assert store.configs[PLUGIN_ID]["whitelist"] == "old"
    assert [item["id"] for item in store.data[(PLUGIN_ID, "profiles")]] == ["legacy"]
    assert store.configs[LEGACY_ID][p._LEGACY_MIGRATED_KEY]


def test_start_discovery_rejects_template_key_for_another_base_url(plugin):
    p, store, _ = plugin
    store.data[(PLUGIN_ID, "profiles")] = [
        profile("p1", base_url="https://a.example/v1", key="sk-a")
    ]

    message = p._start_discovery(base_url="https://evil.example/v1", pid="p1")

    assert "不会复用其 API Key" in message
    assert not store.data.get((PLUGIN_ID, "discovery"))


def test_probe_profile_ignores_result_if_template_changed(plugin, monkeypatch):
    p, store, _ = plugin
    store.data[(PLUGIN_ID, "profiles")] = [profile("p1", name="模板一")]

    def changing_probe(_llm):
        items = store.data[(PLUGIN_ID, "profiles")]
        items[0]["snapshot"]["llm"]["LLM_MODEL"] = "gpt-new"
        store.data[(PLUGIN_ID, "profiles")] = items
        return True, "OK", 1

    monkeypatch.setattr(p, "_probe", changing_probe)

    ok, message = p._probe_profile("p1")

    assert not ok
    assert "已变更" in message
    assert store.data[(PLUGIN_ID, "profiles")][0]["health"]["status"] == "unknown"


def test_apply_llm_config_does_not_clear_current_key(plugin):
    p, _store, settings = plugin

    changed = p._apply_llm_config({"LLM_MODEL": "gpt-new", "LLM_API_KEY": None})

    assert changed
    assert settings.LLM_MODEL == "gpt-new"
    assert settings.LLM_API_KEY == "sk-current"


def test_do_apply_allows_same_endpoint_without_template_key(plugin):
    p, store, _ = plugin
    p._include_credentials = False
    store.data[(PLUGIN_ID, "profiles")] = [
        profile("p1", name="聚合端点", model="gpt-b", key="", base_url="https://current.example/v1")
    ]
    p._apply_llm_config = Mock(return_value=True)

    message = p._do_apply("p1")

    assert "已切换" in message
    written = p._apply_llm_config.call_args.args[0]
    assert "LLM_API_KEY" not in written


def test_sanitize_masks_known_secret_and_authorization(plugin):
    p, _store, settings = plugin

    text = f"Authorization: Bearer {settings.LLM_API_KEY} api_key={settings.LLM_API_KEY}"
    cleaned = p._sanitize(text)

    assert settings.LLM_API_KEY not in cleaned
    assert cleaned.count("***") >= 2


def test_do_apply_ignores_old_template_key_when_credentials_disabled(plugin):
    p, store, _ = plugin
    p._include_credentials = False
    store.data[(PLUGIN_ID, "profiles")] = [
        profile("p1", provider="anthropic", base_url="https://other.example/v1", key="sk-old")
    ]
    p._apply_llm_config = Mock(return_value=True)

    with pytest.raises(RuntimeError, match="拒绝"):
        p._do_apply("p1")

    p._apply_llm_config.assert_not_called()


def test_probe_current_success_resets_fail_count_and_health(plugin, monkeypatch):
    p, store, _settings = plugin
    p._runtime["fail_count"] = 2
    store.data[(PLUGIN_ID, "profiles")] = [profile(
        "p1", name="当前", model="gpt-current",
        base_url="https://current.example/v1", key="sk-current")]

    class FakeLLMHelper:
        @staticmethod
        async def test_current_settings(**kwargs):
            return {"reply_preview": "OK", "duration_ms": 7}

    import app.agent.llm.helper as helper_mod

    monkeypatch.setattr(helper_mod, "LLMHelper", FakeLLMHelper)

    ok, _message, duration = p._probe_current()

    assert ok
    assert duration == 7
    assert p._runtime["fail_count"] == 0
    assert p._runtime["fail_endpoint"] == ""
    assert store.data[(PLUGIN_ID, "profiles")][0]["health"]["status"] == "ok"



def test_plugin_identity_keeps_existing_configuration_namespace():
    """
    更名后保留已有插件 ID 和配置前缀，避免丢失已保存模板。
    """
    assert agentconfigprofile.__name__ == PLUGIN_ID
    assert agentconfigprofile.plugin_config_prefix == "agentconfigprofile_"
    assert agentconfigprofile._LEGACY_PLUGIN_ID == LEGACY_ID


def test_market_metadata_matches_mp_switch_source():
    """
    插件市场和源码使用相同的名称、说明及版本。
    """
    repository = Path(__file__).resolve().parents[3]
    metadata = json.loads((repository / "package.v3.json").read_text(encoding="utf-8"))[PLUGIN_ID]
    expected_description = (
        "参考CC Switch制作，仓库地址"
        "https://github.com/farion1231/cc-switch?tab=readme-ov-file"
    )
    assert agentconfigprofile.plugin_name == metadata["name"] == "MP Switch"
    assert agentconfigprofile.plugin_desc == metadata["description"] == expected_description
    assert agentconfigprofile.plugin_version == metadata["version"]


def test_regressions_load_distribution_source_instead_of_installed_copy():
    """
    回归测试加载插件仓发布源码，避免误测宿主中尚未更新的副本。
    """
    repository = Path(__file__).resolve().parents[3]
    assert Path(mod.__file__).resolve() == repository / "plugins.v3" / PLUGIN_ID / "__init__.py"


def test_cc_switch_reuse_ships_license_and_source_record():
    """
    发布目录保留上游版权、MIT 许可和可追溯的源码记录。
    """
    repository = Path(__file__).resolve().parents[3]
    attribution = repository / "plugins.v3" / PLUGIN_ID / "third_party" / "cc-switch"
    license_text = (attribution / "LICENSE").read_text(encoding="utf-8")
    source_record = (attribution / "README.md").read_text(encoding="utf-8")

    assert "MIT License" in license_text
    assert "Copyright (c) 2025 Jason Young" in license_text
    assert "The above copyright notice and this permission notice shall be included" in license_text
    assert "THE SOFTWARE IS PROVIDED \"AS IS\"" in license_text
    assert "https://github.com/farion1231/cc-switch" in source_record
    assert "7569b27af4f485172950fb6989f5643163842bd1" in source_record
    assert "src-tauri/src/proxy/circuit_breaker.rs" in source_record
