"""
验证 CC Switch 式模式分离、模型永久身份和代理进入退出的配置边界。
"""

from copy import deepcopy

import pytest

from . import test_plugin as regression
from .test_plugin import PLUGIN_ID, profile


class FakeGateway:
    """
    只模拟监听就绪和关闭，不创建真实网络服务。
    """

    def __init__(self):
        self.stopped = False

    def start(self):
        return {"base_url": "http://127.0.0.1:21999/v1", "api_key": "gateway-test-token"}

    def stop(self):
        self.stopped = True


@pytest.fixture(name="plugin")
def mode_plugin(monkeypatch):
    """
    复用内存配置，为模式管理提供两个具有独立密钥的供应商。
    """
    p, store, settings = regression.plugin.__wrapped__(monkeypatch)
    store.data[(PLUGIN_ID, "profiles")] = [
        profile("one", name="Alpha", model="model-a", key="key-a"),
        profile("two", name="Beta", model="model-b", base_url="https://b.example/v1", key="key-b"),
    ]
    gateway = FakeGateway()
    p._gateway_factory = lambda: gateway
    return p, store, settings, gateway


def test_mode_tabs_only_change_view(plugin):
    p, _store, settings, _gateway = plugin
    p._set_mode_view("stack")
    assert p._mode_view() == "stack"
    assert p._mode_active() == "direct"
    assert not settings.writes


def test_invalid_mode_is_rejected_before_settings_write(plugin):
    p, _store, settings, _gateway = plugin
    with pytest.raises(ValueError):
        p._activate_mode("bogus")
    assert not settings.writes


def test_route_switch_preserves_direct_config_and_keeps_gateway_contract(plugin):
    p, _store, settings, gateway = plugin
    original = deepcopy(p._current_llm_config())
    p._activate_mode("route")
    contract = {key: getattr(settings, key) for key in original}
    assert settings.LLM_BASE_URL == "http://127.0.0.1:21999/v1"
    assert settings.LLM_API_KEY == "gateway-test-token"
    assert settings.LLM_USE_PROXY is False
    assert settings.LLM_BASE_URL_PRESET == ""
    p._select_route("two")
    assert p._mode_status()["route_id"] == "two"
    assert {key: getattr(settings, key) for key in original} == contract
    assert p.gateway_candidates("model-from-client")[0]["llm"]["LLM_MODEL"] == "model-b"
    p._activate_mode("direct")
    assert p._current_llm_config() == original
    assert gateway.stopped


def test_route_failure_does_not_publish_mode(plugin):
    p, _store, settings, gateway = plugin
    original = deepcopy(p._current_llm_config())
    settings.locked.add("LLM_BASE_URL")
    with pytest.raises(RuntimeError):
        p._activate_mode("route")
    assert p._mode_active() == "direct"
    assert p._current_llm_config() == original
    assert gateway.stopped


def test_stack_models_route_to_fixed_provider_without_failover(plugin):
    p, _store, _settings, _gateway = plugin
    p._activate_mode("stack")
    p._toggle_stack("two")
    catalog = p._gateway_catalog()
    second = next(item for item in catalog if item["provider_id"] == "two")
    assert second["id"].startswith("ccs-")
    p._auto_failover = True
    candidates = p.gateway_candidates(second["id"])
    assert [item["id"] for item in candidates] == ["two"]
    assert candidates[0]["llm"]["LLM_MODEL"] == "model-b"
    assert len(p.gateway_candidates("ordinary-model")) == 1


def test_stack_removed_member_old_id_is_rejected_and_key_is_reserved(plugin):
    p, store, _settings, _gateway = plugin
    p._activate_mode("stack")
    p._toggle_stack("two")
    original = next(item for item in p._gateway_catalog() if item["provider_id"] == "two")["id"]
    p._toggle_stack("two")
    with pytest.raises(ValueError):
        p.gateway_candidates(original)
    stored_keys = dict(p._mode_status()["stack_keys"])
    p._toggle_stack("two")
    assert original in {item["id"] for item in p._gateway_catalog()}
    assert p._mode_status()["stack_keys"] == stored_keys
    store.data[(PLUGIN_ID, "profiles")] = [profile("new", name="Beta", model="new-model", key="new-key")]
    with pytest.raises(ValueError):
        p.gateway_candidates(original)


def test_stack_default_cannot_be_removed(plugin):
    p, _store, _settings, _gateway = plugin
    p._activate_mode("stack")
    default = p._mode_status()["route_id"]
    with pytest.raises(ValueError):
        p._toggle_stack(default)


def test_stack_unknown_reserved_model_does_not_fall_back(plugin):
    p, _store, _settings, _gateway = plugin
    p._activate_mode("stack")
    with pytest.raises(ValueError):
        p.gateway_candidates("ccs-unknown/model")


def test_stack_model_selection_changes_alias_only(plugin):
    p, _store, settings, _gateway = plugin
    p._activate_mode("stack")
    p._toggle_stack("two")
    item = next(item for item in p._gateway_catalog() if item["provider_id"] == "two")
    p._select_stack_model(item["id"])
    assert settings.LLM_MODEL == item["id"]
    assert settings.LLM_API_KEY == "gateway-test-token"
    assert p.gateway_candidates(settings.LLM_MODEL)[0]["llm"]["LLM_API_KEY"] == "key-b"


def test_stop_restores_owned_gateway_settings(plugin):
    p, _store, settings, gateway = plugin
    original_url = settings.LLM_BASE_URL
    p._activate_mode("route")
    p._stop_mode_gateway()
    assert settings.LLM_BASE_URL == original_url
    assert gateway.stopped
    assert p._mode_active() == "route"
    assert not p._mode_ready()


def test_stop_does_not_overwrite_manually_replaced_settings(plugin):
    p, _store, settings, gateway = plugin
    p._activate_mode("route")
    settings.LLM_BASE_URL = "https://manual.example/v1"
    p._stop_mode_gateway()
    assert settings.LLM_BASE_URL == "https://manual.example/v1"
    assert gateway.stopped


def test_stack_late_result_cannot_change_default_route(plugin):
    p, _store, _settings, _gateway = plugin
    p._activate_mode("route")
    late = p.gateway_candidates("plain")[0]
    late["_gateway_revision"] = p.gateway_mode_revision()
    p._activate_mode("stack")
    before = p._mode_status()["route_id"]
    p.gateway_record(late, True)
    assert p._mode_status()["route_id"] == before


def test_route_apply_uses_pointer_without_replacing_gateway_settings(plugin):
    p, _store, settings, _gateway = plugin
    p._activate_mode("route")
    before = deepcopy(p._raw_llm_config())
    p._do_apply("two")
    assert p._mode_status()["route_id"] == "two"
    assert p._raw_llm_config() == before
    assert settings.LLM_API_KEY == "gateway-test-token"


def test_proxy_snapshot_saves_real_route_not_gateway_credentials(plugin):
    p, _store, _settings, _gateway = plugin
    p._activate_mode("route")
    p._select_route("two")
    snapshot = p._capture_snapshot()["snapshot"]["llm"]
    assert snapshot["LLM_API_KEY"] == "key-b"
    assert snapshot["LLM_BASE_URL"] == "https://b.example/v1"


def test_halfopen_request_permit_is_released_when_mode_changes(plugin):
    p, _store, _settings, _gateway = plugin
    p._activate_mode("route")
    p._auto_failover = True
    request = p.gateway_candidates("model")[0]
    circuit = p._circuit_for(request["llm"])
    from agentconfigprofile.circuit_breaker import CircuitBreakerConfig
    circuit.update_config(CircuitBreakerConfig(failure_threshold=1, timeout_seconds=0))
    circuit.record_failure()
    request["_gateway_revision"] = p.gateway_mode_revision()
    assert p.gateway_permit(request)
    assert circuit.get_stats().half_open_requests == 1
    p._activate_mode("stack")
    p.gateway_record(request, True)
    assert circuit.get_stats().half_open_requests == 0


def test_reload_restores_direct_before_restarting_gateway(plugin):
    p, _store, settings, gateway = plugin
    p._activate_mode("route")
    p._stop_mode_gateway()
    assert settings.LLM_BASE_URL == "https://current.example/v1"
    p._init_modes()
    assert p._mode_active() == "route"
    assert p._mode_ready()
    assert settings.LLM_BASE_URL == "http://127.0.0.1:21999/v1"


def test_unregistered_pending_mode_does_not_keep_dead_gateway_config(plugin):
    p, store, settings, _gateway = plugin
    p._activate_mode("route")
    state = deepcopy(p._mode_state())
    state["active"] = "direct"
    state["pending"] = {"mode": "route"}
    store.data[(PLUGIN_ID, "switch_mode")] = state
    p._gateway = None
    p._init_modes()
    assert p._mode_active() == "direct"
    assert settings.LLM_BASE_URL == "https://current.example/v1"


def test_global_options_in_route_keep_host_on_local_gateway(plugin):
    p, _store, settings, _gateway = plugin
    p._activate_mode("route")
    p._g_use_proxy = True
    p._g_api_protocol = "responses"
    p._g_web_search_mode = "builtin"
    p._apply_global_options()
    assert settings.LLM_USE_PROXY is False
    assert settings.LLM_API_PROTOCOL == "chat_completions"
    assert settings.LLM_WEB_SEARCH_MODE == "local"


def test_discovery_keyless_route_uses_real_endpoint_key_not_gateway_token(plugin):
    p, _store, _settings, _gateway = plugin
    p._activate_mode("route")
    assert p._resolve_discovery_key("https://a.example/v1") == "key-a"


def test_exit_keyless_direct_profile_restores_real_credential(plugin):
    p, store, settings, _gateway = plugin
    keyless = profile("current", model=settings.LLM_MODEL, base_url=settings.LLM_BASE_URL, key="")
    store.data[(PLUGIN_ID, "profiles")].insert(0, keyless)
    p._activate_mode("route")
    p._activate_mode("direct")
    assert settings.LLM_API_KEY == "sk-current"
    assert settings.LLM_BASE_URL == "https://current.example/v1"


def test_mode_persistence_failure_restores_previous_system_connection(plugin):
    p, _store, settings, gateway = plugin
    original = deepcopy(p._raw_llm_config())
    save = p.save_data
    failed = []
    def failing_save(key, data, **kwargs):
        if key == "switch_mode" and data.get("active") == "route" and not data.get("pending") and not failed:
            failed.append(True)
            raise RuntimeError("mode data unavailable")
        return save(key, data, **kwargs)
    p.save_data = failing_save
    with pytest.raises(RuntimeError, match="mode data unavailable"):
        p._activate_mode("route")
    assert p._raw_llm_config() == original
    assert p._mode_active() == "direct"
    assert gateway.stopped


def test_removing_selected_stack_member_requires_selecting_another_model(plugin):
    p, _store, settings, _gateway = plugin
    p._activate_mode("stack")
    p._toggle_stack("two")
    alias = next(item["id"] for item in p._gateway_catalog() if item["provider_id"] == "two")
    p._select_stack_model(alias)
    with pytest.raises(ValueError, match="模型"):
        p._toggle_stack("two")
    assert settings.LLM_MODEL == alias
    assert p.gateway_candidates(alias)[0]["id"] == "two"


def test_route_contract_uses_minimum_limits_of_supported_candidates(plugin):
    p, store, settings, _gateway = plugin
    items = store.data[(PLUGIN_ID, "profiles")]
    items[0]["snapshot"]["llm"]["LLM_MAX_CONTEXT_TOKENS"] = 16
    items[0]["snapshot"]["llm"]["LLM_SUPPORT_IMAGE_INPUT"] = False
    items[1]["snapshot"]["llm"]["LLM_MAX_CONTEXT_TOKENS"] = 128
    items[1]["snapshot"]["llm"]["LLM_SUPPORT_IMAGE_INPUT"] = True
    p._activate_mode("route")
    assert settings.LLM_MAX_CONTEXT_TOKENS == 16
    assert settings.LLM_SUPPORT_IMAGE_INPUT is False


@pytest.mark.parametrize("operation", ["stop", "direct"])
def test_gateway_shutdown_occurs_after_plugin_locks_are_released(plugin, operation):
    p, _store, _settings, gateway = plugin
    p._activate_mode("route")
    import threading
    acquired = []
    def stop():
        def finish_request():
            with p._apply_lock, p._lock:
                acquired.append(True)
        thread = threading.Thread(target=finish_request)
        thread.start()
        thread.join(timeout=1)
        assert acquired, "gateway cleanup is waiting on plugin state locks"
        gateway.stopped = True
    gateway.stop = stop
    if operation == "stop":
        p._stop_mode_gateway()
    else:
        p._activate_mode("direct")
    assert acquired


def test_edited_direct_endpoint_without_key_does_not_inherit_old_endpoint_secret(plugin):
    p, store, settings, _gateway = plugin
    current = profile("current", model=settings.LLM_MODEL, base_url=settings.LLM_BASE_URL, key=settings.LLM_API_KEY)
    store.data[(PLUGIN_ID, "profiles")].insert(0, current)
    p._activate_mode("route")
    saved = store.data[(PLUGIN_ID, "profiles")][0]
    saved["snapshot"]["llm"]["LLM_BASE_URL"] = "https://changed.example/v1"
    saved["snapshot"]["llm"].pop("LLM_API_KEY")
    with pytest.raises(RuntimeError, match="密钥"):
        p._activate_mode("direct")
    assert settings.LLM_BASE_URL == "http://127.0.0.1:21999/v1"


def test_explicitly_cleared_direct_endpoint_cannot_reuse_custom_secret(plugin):
    p, store, settings, _gateway = plugin
    current = profile("current", model=settings.LLM_MODEL, base_url=settings.LLM_BASE_URL, key=settings.LLM_API_KEY)
    store.data[(PLUGIN_ID, "profiles")].insert(0, current)
    p._activate_mode("route")
    saved = store.data[(PLUGIN_ID, "profiles")][0]
    saved["snapshot"]["llm"]["LLM_BASE_URL"] = ""
    saved["snapshot"]["llm"].pop("LLM_API_KEY")
    with pytest.raises(RuntimeError, match="密钥"):
        p._activate_mode("direct")
    assert settings.LLM_API_KEY == "gateway-test-token"


def test_stack_selection_is_validated_when_reentering_after_member_removal(plugin):
    p, _store, settings, _gateway = plugin
    p._activate_mode("stack")
    p._toggle_stack("two")
    alias = next(item["id"] for item in p._gateway_catalog() if item["provider_id"] == "two")
    p._select_stack_model(alias)
    p._activate_mode("route")
    p._toggle_stack("two")
    p._activate_mode("stack")
    assert settings.LLM_MODEL != alias
    assert len(p.gateway_candidates(settings.LLM_MODEL)) == 1


def test_stop_still_closes_listener_if_persistence_fails(plugin):
    p, _store, _settings, gateway = plugin
    p._activate_mode("route")
    def fail_save(*_args, **_kwargs):
        raise RuntimeError("mode data unavailable")
    p.save_data = fail_save
    with pytest.raises(RuntimeError):
        p._stop_mode_gateway()
    assert gateway.stopped


def test_real_gateway_integrates_route_and_stack_model_selection(plugin):
    p, _store, settings, _gateway = plugin
    import httpx
    from agentconfigprofile.gateway import LocalGateway
    from langchain_core.messages import AIMessage

    from .test_gateway import MemoryLLM
    requested = []
    def factory(selected, _options):
        requested.append((selected["id"], selected["llm"]["LLM_MODEL"], selected["llm"]["LLM_API_KEY"]))
        return MemoryLLM(reply=AIMessage(content=selected["id"]))
    local = LocalGateway(p, transport_factory=factory)
    p._gateway_factory = lambda: local
    try:
        p._activate_mode("route")
        with httpx.Client(trust_env=False, timeout=3) as client:
            headers = {"Authorization": f"Bearer {settings.LLM_API_KEY}"}
            def chat(model):
                return client.post(f"{settings.LLM_BASE_URL}/chat/completions", headers=headers,
                                   json={"model": model, "messages": [{"role": "user", "content": "hello"}]})
            assert chat(settings.LLM_MODEL).json()["choices"][0]["message"]["content"] == "one"
            p._select_route("two")
            assert chat(settings.LLM_MODEL).json()["choices"][0]["message"]["content"] == "two"
            p._activate_mode("stack")
            p._toggle_stack("one")
            alias = next(item["id"] for item in p._gateway_catalog() if item["provider_id"] == "one")
            p._select_stack_model(alias)
            assert chat(alias).json()["choices"][0]["message"]["content"] == "one"
            assert chat("ccs-missing/model").status_code == 404
        assert requested == [("one", "model-a", "key-a"), ("two", "model-b", "key-b"),
                             ("one", "model-a", "key-a")]
    finally:
        p._stop_mode_gateway()
