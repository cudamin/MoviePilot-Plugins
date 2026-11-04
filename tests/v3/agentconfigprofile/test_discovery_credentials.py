"""
验证模型发现和结果导入不会把其他端点的密钥用于当前地址。
"""

import pytest

from . import test_plugin as regression
from .test_plugin import PLUGIN_ID


@pytest.fixture(name="plugin")
def discovery_plugin(monkeypatch):
    """
    使用内存配置和数据存储，避免实际网络与持久化副作用。
    """
    return regression.plugin.__wrapped__(monkeypatch)


@pytest.mark.parametrize("key_source", ["runtime", "configured"])
def test_discovery_rejects_key_bound_to_another_endpoint(plugin, key_source):
    """
    地址改变后不得复用之前的发现密钥或配置中的发现密钥。
    """
    p, _store, _settings = plugin
    if key_source == "runtime":
        p._discovery_api_key_runtime = "key-for-a"
        p._discovery_base_url_runtime = "https://a.example/v1"
    else:
        p._discover_api_key = "key-for-a"
        p._discover_base_url = "https://a.example/v1"

    assert p._resolve_discovery_key("https://b.example/v1") == ""


@pytest.mark.parametrize("key_source", ["runtime", "configured"])
def test_discovery_keeps_key_for_its_own_endpoint(plugin, key_source):
    """
    同一端点仍可复用所属密钥，避免阻断正常的重复发现和导入。
    """
    p, _store, _settings = plugin
    if key_source == "runtime":
        p._discovery_api_key_runtime = "key-for-a"
        p._discovery_base_url_runtime = "https://a.example/v1"
    else:
        p._discover_api_key = "key-for-a"
        p._discover_base_url = "https://a.example/v1"

    assert p._resolve_discovery_key("https://a.example/v1") == "key-for-a"


def test_explicit_key_and_current_endpoint_keep_working(plugin):
    """
    用户显式传入的密钥优先，同一当前系统端点仍可继承系统密钥。
    """
    p, _store, settings = plugin
    p._discovery_api_key_runtime = "key-for-a"
    p._discovery_base_url_runtime = "https://a.example/v1"

    assert p._resolve_discovery_key("https://b.example/v1", "key-for-b") == "key-for-b"
    assert p._resolve_discovery_key(settings.LLM_BASE_URL) == settings.LLM_API_KEY


def test_discovered_models_cannot_inherit_previous_endpoint_key(plugin):
    """
    B 的模型结果不能因残留 A 的发现密钥而生成带 A 凭据的配置。
    """
    p, store, _settings = plugin
    p._discovery_api_key_runtime = "key-for-a"
    p._discovery_base_url_runtime = "https://a.example/v1"
    store.data[(PLUGIN_ID, "discovery")] = {
        "status": "done", "base_url": "https://b.example/v1",
        "providers": {"openai": {"ok": True, "models": [{"id": "model-b"}]}},
    }

    result = p._import_discovered()

    assert "导入失败" in result
    assert not store.data.get((PLUGIN_ID, "profiles"))
