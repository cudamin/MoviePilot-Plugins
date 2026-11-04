"""
验证重构配置页保持旧配置合同、响应式布局和输入提示。
"""

from __future__ import annotations

from collections import Counter

from agentconfigprofile import agentconfigprofile
from agentconfigprofile.forms import build_form
from agentconfigprofile.presets import get_user_agent_presets

EXPECTED_DEFAULTS = {
    "_active_tab": "basic", "enabled": False, "include_credentials": True, "notify": True,
    "auto_failover": False, "takeover": True, "auto_recover": False,
    "check_cron": "*/30 * * * *", "fail_threshold": 2, "test_timeout": 20, "probe_prompt": "请只回复 OK",
    "discover_base_url": "", "discover_api_key": "", "discover_providers": ["openai", "anthropic", "deepseek"],
    "discover_filter": "", "discover_limit": 20, "discover_auto_import": True, "discover_smart_match": True,
    "whitelist": "", "apply_global": False, "g_thinking_level": "off", "g_use_proxy": True,
    "g_api_protocol": "auto", "g_web_search_mode": "local", "g_user_agent": "", "g_job_interval": 0,
    "g_image_input": True, "g_audio_input": False, "g_audio_output": False,
    "g_retry_transfer": False, "g_ai_recommend": False, "action": "none", "new_profile_name": "",
}


def _nodes(nodes):
    for node in nodes:
        yield node
        yield from _nodes(node.get("content", []))


def _form():
    return build_form(object.__new__(agentconfigprofile))


def test_rebuilt_form_keeps_all_legacy_defaults_and_model_keys():
    form, defaults = _form()
    assert defaults == EXPECTED_DEFAULTS
    models = Counter(node.get("props", {}).get("model") for node in _nodes(form))
    for key in EXPECTED_DEFAULTS:
        assert models[key] == (2 if key == "_active_tab" else 1)


def test_form_keeps_four_tab_values_with_matching_panels():
    form, _defaults = _form()
    tabs = [node for node in _nodes(form) if node["component"] == "VTab"]
    windows = [node for node in _nodes(form) if node["component"] == "VWindowItem"]
    assert [(node["props"]["value"], node["text"]) for node in tabs] == [
        ("basic", "基础"), ("failover", "自动切换"), ("discover", "模型发现"), ("global", "全局参数"),
    ]
    assert [node["props"]["value"] for node in windows] == ["basic", "failover", "discover", "global"]


def test_form_preserves_protocol_and_action_value_contracts():
    form, _defaults = _form()
    inputs = {node["props"]["model"]: node["props"] for node in _nodes(form)
              if node.get("props", {}).get("model") not in {None, "_active_tab"}}
    assert [item["value"] for item in inputs["action"]["items"]] == ["none", "save", "discover"]
    assert [item["value"] for item in inputs["discover_providers"]["items"]] == list(agentconfigprofile._DISCOVER_PROVIDERS)
    assert inputs["discover_providers"]["multiple"] is True
    assert [item["value"] for item in inputs["g_api_protocol"]["items"]] == ["auto", "chat_completions", "responses"]
    assert [item["value"] for item in inputs["g_job_interval"]["items"]] == [0] + list(agentconfigprofile._JOB_INTERVAL_OPTIONS)
    assert inputs["discover_api_key"]["type"] == "password"
    assert inputs["discover_api_key"]["autocomplete"] == "new-password"
    assert inputs["g_user_agent"]["items"] == list(get_user_agent_presets())
    assert inputs["g_user_agent"]["clearable"] is True


def test_form_stacks_columns_on_mobile_and_wraps_long_description():
    form, _defaults = _form()
    columns = [node for node in _nodes(form) if node["component"] == "VCol"]
    assert columns
    assert all(node["props"]["cols"] == 12 for node in columns)
    description = next(node for node in _nodes(form) if node.get("text") == agentconfigprofile.plugin_desc)
    assert "overflow-wrap: anywhere" in description["props"]["style"]
    assert all("width" not in node["props"] for node in columns)


def test_form_has_hints_without_reading_current_settings():
    plugin = object.__new__(agentconfigprofile)
    form, _defaults = build_form(plugin)
    inputs = [node for node in _nodes(form) if node.get("props", {}).get("model") not in {None, "_active_tab"}]
    assert all("value" not in node["props"] for node in inputs)
    hinted = [node for node in inputs if node["props"].get("hint")]
    assert hinted
    assert all(node["props"]["persistent-hint"] is True for node in hinted)
