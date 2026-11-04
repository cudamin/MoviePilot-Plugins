"""
验证 MP Switch 移植的熔断状态机、冷却计时和半开许可隔离。
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest
from agentconfigprofile.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
)


class ManualClock:
    """
    可手动推进的单调时钟，避免测试等待真实冷却。

    :ivar now: 当前模拟单调秒数
    """

    def __init__(self) -> None:
        """
        从零秒开始计时。
        """
        self.now = 0.0

    def __call__(self) -> float:
        """
        返回当前模拟单调秒数。

        :return: 当前秒数
        """
        return self.now


@pytest.fixture
def breaker_pair():
    """
    返回使用可推进时钟的默认熔断器。

    :return: 熔断器及模拟时钟
    """
    clock = ManualClock()
    return CircuitBreaker(clock=clock), clock


def open_breaker(breaker: CircuitBreaker, failures: int = 4) -> None:
    """
    连续失败达到阈值以启动冷却。

    :param breaker: 待测试熔断器
    :param failures: 需要记录的失败次数
    """
    for _ in range(failures):
        breaker.record_failure()
    assert breaker.get_state() == CircuitState.OPEN


def test_default_configuration_matches_upstream():
    """
    上游默认参数在跨语言移植时保持一致。
    """
    config = CircuitBreakerConfig()
    assert (config.failure_threshold, config.success_threshold, config.timeout_seconds) == (4, 2, 60)
    assert (config.error_rate_threshold, config.min_requests) == (0.6, 10)


@pytest.mark.parametrize(
    "changes",
    [
        {"failure_threshold": 0},
        {"success_threshold": 0},
        {"min_requests": 0},
        {"timeout_seconds": -1},
        {"error_rate_threshold": -0.1},
        {"error_rate_threshold": 1.1},
    ],
)
def test_invalid_configuration_is_rejected(changes):
    """
    参数越界时拒绝构造，避免永不恢复的状态。
    """
    with pytest.raises(ValueError):
        CircuitBreakerConfig(**changes)


def test_success_interrupts_consecutive_failure_streak(breaker_pair):
    """
    成功请求中断连续失败计数，但保留累计请求统计。
    """
    breaker, _clock = breaker_pair
    breaker.record_failure()
    breaker.record_failure()
    breaker.record_success()
    breaker.record_failure()
    stats = breaker.get_stats()
    assert stats.state == CircuitState.CLOSED
    assert stats.consecutive_failures == 1
    assert (stats.total_requests, stats.failed_requests) == (4, 3)


def test_open_cooldown_and_two_successful_recovery_probes(breaker_pair):
    """
    冷却未结束时拒绝请求，半开连续成功达到阈值后恢复。
    """
    breaker, clock = breaker_pair
    open_breaker(breaker)
    clock.now = 59.99
    assert not breaker.is_available()
    assert not breaker.allow_request().allowed
    clock.now = 60
    assert breaker.is_available()
    assert breaker.get_state() == CircuitState.HALF_OPEN
    permit = breaker.allow_request()
    assert permit.allowed and permit.used_half_open_permit
    assert not breaker.allow_request().allowed
    breaker.record_success(permit.used_half_open_permit)
    assert breaker.get_state() == CircuitState.HALF_OPEN
    permit = breaker.allow_request()
    assert permit.allowed
    breaker.record_success(permit.used_half_open_permit)
    stats = breaker.get_stats()
    assert stats.state == CircuitState.CLOSED
    assert (stats.total_requests, stats.failed_requests, stats.half_open_requests) == (0, 0, 0)


def test_half_open_failure_restarts_full_cooldown(breaker_pair):
    """
    恢复探测失败后从失败时间重新计算完整冷却周期。
    """
    breaker, clock = breaker_pair
    open_breaker(breaker)
    clock.now = 60
    permit = breaker.allow_request()
    breaker.record_failure(permit.used_half_open_permit)
    assert breaker.get_state() == CircuitState.OPEN
    clock.now = 119.99
    assert not breaker.is_available()
    clock.now = 120
    assert breaker.allow_request().allowed


def test_error_rate_requires_minimum_completed_requests():
    """
    错误率只在达到最小请求数后计算，与连续失败阈值独立。
    """
    breaker = CircuitBreaker(CircuitBreakerConfig(failure_threshold=20, min_requests=5))
    breaker.record_failure()
    breaker.record_success()
    breaker.record_failure()
    breaker.record_success()
    assert breaker.get_state() == CircuitState.CLOSED
    breaker.record_failure()
    assert breaker.get_state() == CircuitState.OPEN
    assert breaker.get_stats().failed_requests == 3


def test_availability_checks_do_not_consume_half_open_permit(breaker_pair):
    """
    多次候选检查不会占用或重置恢复探测的许可。
    """
    breaker, clock = breaker_pair
    open_breaker(breaker)
    clock.now = 60
    for _ in range(3):
        assert breaker.is_available()
    permit = breaker.allow_request()
    assert permit.allowed
    assert breaker.is_available()
    assert not breaker.allow_request().allowed


def test_cancelled_request_releases_permit_without_changing_health(breaker_pair):
    """
    请求取消后释放半开许可，不计入失败率或恢复次数。
    """
    breaker, clock = breaker_pair
    open_breaker(breaker)
    clock.now = 60
    permit = breaker.allow_request()
    before = breaker.get_stats()
    breaker.release_permit_neutral(permit.used_half_open_permit)
    after = breaker.get_stats()
    assert after == replace(before, half_open_requests=0)
    assert breaker.allow_request().allowed


def test_only_one_concurrent_half_open_request_is_admitted(breaker_pair):
    """
    并行线程竞争半开许可时只允许一个实际恢复请求。
    """
    breaker, clock = breaker_pair
    open_breaker(breaker)
    clock.now = 60
    barrier = threading.Barrier(12)

    def acquire():
        barrier.wait()
        return breaker.allow_request()

    with ThreadPoolExecutor(max_workers=12) as executor:
        permits = list(executor.map(lambda _index: acquire(), range(12)))
    assert sum(permit.allowed for permit in permits) == 1
    assert sum(permit.used_half_open_permit for permit in permits) == 1


def test_configuration_update_preserves_existing_state(breaker_pair):
    """
    热更新缩短冷却且不丢失已累计的健康结果。
    """
    breaker, clock = breaker_pair
    open_breaker(breaker)
    breaker.update_config(CircuitBreakerConfig(timeout_seconds=10))
    assert breaker.get_stats().failed_requests == 4
    clock.now = 10
    assert breaker.allow_request().allowed


def test_manual_reset_does_not_revoke_in_flight_permit(breaker_pair):
    """
    手动恢复与旧探测完成交错时，旧许可仍阻止重复恢复请求。
    """
    breaker, clock = breaker_pair
    open_breaker(breaker)
    clock.now = 60
    permit = breaker.allow_request()
    breaker.reset()
    open_breaker(breaker)
    clock.now = 120
    assert breaker.is_available()
    assert not breaker.allow_request().allowed
    breaker.release_permit_neutral(permit.used_half_open_permit)
    assert breaker.allow_request().allowed


def test_zero_cooldown_allows_immediate_recovery():
    """
    零秒冷却配置可立即发出恢复探测。
    """
    breaker = CircuitBreaker(CircuitBreakerConfig(timeout_seconds=0))
    open_breaker(breaker)
    assert breaker.allow_request().used_half_open_permit
