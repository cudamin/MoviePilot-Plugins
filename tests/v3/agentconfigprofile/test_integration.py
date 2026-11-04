"""
验证 MP Switch 队列、探活、熔断和受鉴权保护的配置操作之间的协作。

所有 LLM 调用使用内存模拟，熔断时钟由测试推进，不访问真实设置或网络。
"""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import Mock

import pytest
from agentconfigprofile.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
)

from . import test_plugin as regression
from .test_circuit_breaker import ManualClock
from .test_plugin import (
    PLUGIN_ID,
    llm,
    profile,
)


@pytest.fixture(name="plugin")
def integration_plugin_fixture(monkeypatch):
    """
    复用插件回归夹具中的内存设置和存储边界。

    :param monkeypatch: 自动撤销设置替换的 pytest 夹具
    :return: 插件实例、内存存储和内存设置
    """
    return regression.plugin.__wrapped__(monkeypatch)


def opened_circuit(plugin_instance, configuration):
    """
    为指定配置安装达到失败阈值的独立熔断器。

    :param plugin_instance: 使用内存设置的插件实例
    :param configuration: 将作为独立熔断身份的 LLM 配置
    :return: 熔断器及可推进的测试时钟
    """
    clock = ManualClock()
    circuit = CircuitBreaker(
        CircuitBreakerConfig(failure_threshold=plugin_instance._fail_threshold), clock=clock,
    )
    if not hasattr(plugin_instance, "_circuits"):
        plugin_instance._circuits = {}
    plugin_instance._circuits[plugin_instance._fingerprint(configuration)] = circuit
    for _index in range(plugin_instance._fail_threshold):
        circuit.record_failure()
    assert circuit.get_state() == CircuitState.OPEN
    return circuit, clock


def test_failover_keeps_queue_order_and_skips_opted_out_profiles(plugin, monkeypatch):
    """
    自动切换严格使用配置顺序，忽略移出队列项且不按旧健康状态重排。
    """
    p, store, settings = plugin
    disabled = profile("disabled", model="disabled")
    disabled["in_failover_queue"] = False
    first = profile("first", model="first")
    second = profile("second", model="second")
    third = profile("third", model="third")
    third["health"] = {"status": "ok", "checked_at": p._now()}
    store.data[(PLUGIN_ID, "profiles")] = [disabled, first, second, third]
    requested = []

    def probe(configuration):
        requested.append(configuration["LLM_MODEL"])
        return configuration["LLM_MODEL"] != "first", "模拟探活", 1

    monkeypatch.setattr(p, "_probe", probe)
    result = p._do_failover("当前配置故障")

    assert requested == ["first", "second"]
    assert "已切换" in result
    assert settings.LLM_MODEL == "second"
    assert p._in_failover_queue(first)


def test_current_configuration_is_not_reprobed_as_backup(plugin, monkeypatch):
    """
    队列中的当前配置不作为自己的备用项再次探活。
    """
    p, store, settings = plugin
    current = profile("current", model=settings.LLM_MODEL,
                      base_url=settings.LLM_BASE_URL, key=settings.LLM_API_KEY)
    store.data[(PLUGIN_ID, "profiles")] = [current, profile("backup", model="backup")]
    probe = Mock(return_value=(True, "OK", 1))
    monkeypatch.setattr(p, "_probe", probe)

    result = p._do_failover("当前配置故障")

    assert "已切换" in result
    assert probe.call_count == 1
    assert probe.call_args.args[0]["LLM_MODEL"] == "backup"
    assert settings.LLM_MODEL == "backup"


@pytest.mark.parametrize("target", ["profile", "current"])
def test_open_circuit_skips_real_probe_until_cooldown(plugin, monkeypatch, target):
    """
    冷却期间无论探活模板还是当前配置都不实际发送 LLM 请求。
    """
    p, store, _settings = plugin
    candidate = profile("backup")
    store.data[(PLUGIN_ID, "profiles")] = [candidate]
    configuration = p._profile_llm(candidate) if target == "profile" else p._current_llm_config()
    circuit, clock = opened_circuit(p, configuration)
    probe = Mock(return_value=(True, "OK", 1))
    monkeypatch.setattr(p, "_probe", probe)
    clock.now = 59.99
    before = circuit.get_stats()

    result = p._probe_profile("backup") if target == "profile" else p._probe_current()

    assert not result[0]
    assert "已跳过请求" in result[1]
    probe.assert_not_called()
    assert circuit.get_stats() == before
    assert not p._runtime.get("last_check_at")


def test_failover_requires_two_half_open_successes_before_applying(plugin, monkeypatch):
    """
    同一备用配置在半开状态连续成功两次后才允许写入系统设置。
    """
    p, store, settings = plugin
    candidate = profile("backup", model="backup")
    store.data[(PLUGIN_ID, "profiles")] = [candidate]
    circuit, clock = opened_circuit(p, p._profile_llm(candidate))
    probe = Mock(return_value=(True, "OK", 1))
    monkeypatch.setattr(p, "_probe", probe)
    monkeypatch.setattr(p, "_FAILOVER_COOLDOWN", 0)
    clock.now = 60

    first_result = p._do_failover("当前故障")

    assert "自动切换失败" in first_result
    assert circuit.get_state() == CircuitState.HALF_OPEN
    assert circuit.get_stats().consecutive_successes == 1
    assert settings.LLM_MODEL == "gpt-current"
    assert not settings.writes

    second_result = p._do_failover("当前故障")

    assert "已切换" in second_result
    assert circuit.get_state() == CircuitState.CLOSED
    assert settings.LLM_MODEL == "backup"
    assert probe.call_count == 2


def test_recover_primary_requires_two_successes_and_ignores_opted_out_head(plugin, monkeypatch):
    """
    首选恢复以队列首项为准，半开成功一次不能提前切回。
    """
    p, store, settings = plugin
    excluded = profile("excluded", model="excluded")
    excluded["in_failover_queue"] = False
    candidate = profile("primary", model="primary")
    store.data[(PLUGIN_ID, "profiles")] = [excluded, candidate]
    circuit, clock = opened_circuit(p, p._profile_llm(candidate))
    probe = Mock(return_value=(True, "OK", 1))
    monkeypatch.setattr(p, "_probe", probe)
    clock.now = 60

    assert p._try_recover_primary(p._current_llm_config()) is None
    assert circuit.get_state() == CircuitState.HALF_OPEN
    assert not settings.writes
    assert "已切换" in p._try_recover_primary(p._current_llm_config())
    assert settings.LLM_MODEL == "primary"
    assert probe.call_count == 2


@pytest.mark.parametrize("target", ["profile", "current"])
@pytest.mark.parametrize("interruption", ["cancelled", "changed", "exception"])
def test_interrupted_half_open_probe_releases_permit_without_recording_result(
        plugin, monkeypatch, target, interruption):
    """
    取消、配置变更和探活异常均归还半开许可，不把失效结果计入健康统计。
    """
    p, store, settings = plugin
    candidate = profile("backup")
    store.data[(PLUGIN_ID, "profiles")] = [candidate]
    configuration = p._profile_llm(candidate) if target == "profile" else p._current_llm_config()
    circuit, clock = opened_circuit(p, configuration)
    clock.now = 60
    assert circuit.is_available()
    before = circuit.get_stats()

    def probe(_configuration):
        assert circuit.get_stats().half_open_requests == 1
        if interruption == "cancelled":
            p._stop_event.set()
        elif interruption == "changed":
            if target == "current":
                settings.LLM_MODEL = "changed"
            else:
                store.data[(PLUGIN_ID, "profiles")][0]["snapshot"]["llm"]["LLM_MODEL"] = "changed"
        else:
            raise RuntimeError("模拟请求异常")
        return False, "失效结果", 1

    monkeypatch.setattr(p, "_probe", probe)
    if interruption == "exception":
        with pytest.raises(RuntimeError, match="模拟请求异常"):
            p._probe_profile("backup") if target == "profile" else p._probe_current()
    else:
        result = p._probe_profile("backup") if target == "profile" else p._probe_current()
        assert not result[0]
        assert "停止" in result[1] or "变更" in result[1]

    assert circuit.get_stats() == replace(before, half_open_requests=0)
    assert circuit.allow_request().allowed
    assert store.data[(PLUGIN_ID, "profiles")][0]["health"]["status"] == "unknown"
    assert p._runtime["fail_count"] == 0


def test_circuit_state_isolated_by_key_and_protocol(plugin):
    """
    同模型同端点的不同密钥和请求协议分别累计故障。
    """
    p, _store, _settings = plugin
    first = llm(key="first")
    opened, _clock = opened_circuit(p, first)
    second = p._circuit_for(llm(key="second"))
    different_protocol = p._circuit_for(llm(key="first", protocol="responses"))

    assert p._circuit_for(first) is opened
    assert second is not opened
    assert different_protocol is not opened
    assert second.get_state() == different_protocol.get_state() == CircuitState.CLOSED
    assert second.get_stats().total_requests == 0
    assert different_protocol.get_stats().total_requests == 0


def test_queue_toggle_requires_authentication_and_preserves_snapshot(plugin):
    """
    未通过鉴权无法改队列，通过后可移出和重新加入，其他模板字段保持不变。
    """
    p, store, settings = plugin
    candidate = profile("backup")
    store.data[(PLUGIN_ID, "profiles")] = [candidate]

    denied = p.api_toggle_queue("backup", "invalid-token")

    assert not denied.success
    assert store.data[(PLUGIN_ID, "profiles")] == [candidate]
    removed = p.api_toggle_queue("backup", settings.API_TOKEN)
    assert removed.success
    assert store.data[(PLUGIN_ID, "profiles")][0] == {**candidate, "in_failover_queue": False}
    added = p.api_toggle_queue("backup", settings.API_TOKEN)
    assert added.success
    assert store.data[(PLUGIN_ID, "profiles")][0] == {**candidate, "in_failover_queue": True}
    missing = p.api_toggle_queue("missing", settings.API_TOKEN)
    assert not missing.success
    assert "配置不存在" in missing.message


def test_queue_opt_out_keeps_manual_apply_available(plugin):
    """
    移出自动队列的配置仍允许用户手动应用。
    """
    p, store, settings = plugin
    candidate = profile("backup", model="manual")
    candidate["in_failover_queue"] = False
    store.data[(PLUGIN_ID, "profiles")] = [candidate]

    result = p.api_apply("backup", settings.API_TOKEN)

    assert result.success
    assert settings.LLM_MODEL == "manual"
    assert store.data[(PLUGIN_ID, "profiles")][0]["in_failover_queue"] is False


def test_probe_error_masks_backup_template_key(plugin, monkeypatch):
    """
    备用模板请求的异常文本必须遮盖该模板密钥，即使它与当前系统密钥不同。
    """
    p, store, settings = plugin
    secret = "sk-backup-private-secret"
    candidate = profile("backup", key=secret)
    store.data[(PLUGIN_ID, "profiles")] = [candidate]

    class FailingHelper:
        """
        模拟携带备用密钥的请求异常，不访问网络。
        """

        @staticmethod
        async def test_current_settings(**_kwargs):
            """
            抛出包含请求密钥的异常以检查错误脱敏。
            """
            raise RuntimeError(f"上游拒绝 {secret}")

    import app.agent.llm.helper as helper_mod

    monkeypatch.setattr(helper_mod, "LLMHelper", FailingHelper)
    assert settings.LLM_API_KEY != secret
    ok, message = p._probe_profile("backup")

    assert not ok
    assert secret not in message
    assert "***" in message
    health = store.data[(PLUGIN_ID, "profiles")][0]["health"]
    assert secret not in health["message"]


@pytest.mark.parametrize("invalidation", ["removed", "expired", "open"])
def test_background_apply_rechecks_queue_health_and_circuit(plugin, invalidation):
    """
    自动应用前重新核对队列成员、健康有效期和熔断状态，避免写入过期候选。
    """
    p, store, settings = plugin
    candidate = profile("backup", model="backup")
    candidate["health"] = {"status": "ok", "checked_at": p._now()}
    if invalidation == "removed":
        candidate["in_failover_queue"] = False
    elif invalidation == "expired":
        candidate["health"]["checked_at"] = "2000-01-01 00:00:00"
    else:
        opened_circuit(p, p._profile_llm(candidate))
    store.data[(PLUGIN_ID, "profiles")] = [candidate]

    with pytest.raises(RuntimeError, match="应用失败"):
        p._do_apply("backup", background=True)

    assert not settings.writes
    assert settings.LLM_MODEL == "gpt-current"


def test_keyless_template_circuit_tracks_current_effective_key(plugin):
    """
    未保存密钥的同端点模板使用实际继承的密钥隔离熔断身份。
    """
    p, _store, settings = plugin
    configuration = llm(key="", model=settings.LLM_MODEL, base_url=settings.LLM_BASE_URL)
    before = p._circuit_for(configuration)
    assert before is p._circuit_for({**configuration, "LLM_API_KEY": settings.LLM_API_KEY})
    before.record_failure()
    settings.LLM_API_KEY = "replacement-key"

    after = p._circuit_for(configuration)

    assert after is not before
    assert after.get_stats().total_requests == 0
