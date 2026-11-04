"""
验证 CC Switch 原始 User-Agent 数据复用与显式选择行为。
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from agentconfigprofile import agentconfigprofile
from agentconfigprofile.presets import (
    USER_AGENT_RESOURCE_SHA256,
    _parse_user_agent_presets,
    get_user_agent_presets,
)


def test_user_agent_resource_matches_recorded_upstream_version():
    resource = Path(__file__).parents[3] / "plugins.v3" / "agentconfigprofile" / "third_party" / "cc-switch"
    data = (resource / "userAgentPresets.ts").read_bytes()
    assert hashlib.sha256(data).hexdigest() == USER_AGENT_RESOURCE_SHA256
    assert get_user_agent_presets() == (
        "claude-cli/2.1.161 (external, cli)",
        "claude-cli/2.1.161",
        "claude-code/1.0.0",
        "claude-code/0.1.0",
        "Kilo-Code/1.0",
    )


@pytest.mark.parametrize("array", ['["safe", execute()]', '["safe", 2]', '["safe\\r\\nInjected: true"]', "[]"])
def test_user_agent_parser_rejects_expressions_and_invalid_header_values(array):
    with pytest.raises(ValueError):
        _parse_user_agent_presets(f"export const USER_AGENT_PRESETS: readonly string[] = {array};")


def test_user_agent_parser_requires_expected_export():
    with pytest.raises(ValueError, match="缺少静态数组"):
        _parse_user_agent_presets('const OTHER = ["safe"];')


def _find_user_agent_field(nodes):
    for node in nodes:
        if node.get("props", {}).get("model") == "g_user_agent":
            return node
        found = _find_user_agent_field(node.get("content", []))
        if found:
            return found
    return None


def test_user_agent_form_offers_presets_and_preserves_empty_default():
    plugin = object.__new__(agentconfigprofile)
    form, defaults = plugin.get_form()
    field = _find_user_agent_field(form)
    assert field["component"] == "VCombobox"
    assert field["props"]["items"] == list(get_user_agent_presets())
    assert field["props"]["clearable"] is True
    assert "自定义" in field["props"]["label"]
    assert defaults["g_user_agent"] == ""
    assert "value" not in field["props"]
