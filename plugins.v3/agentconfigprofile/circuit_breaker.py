"""
移植 CC Switch 的熔断状态机，仅管理内存中的供应商健康状态。

源实现为 src-tauri/src/proxy/circuit_breaker.rs，许可与结果保持一一对应。
模块不访问 MoviePilot 设置、数据库或网络，调用方负责请求和许可的最终释放。
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum


class CircuitState(StrEnum):
    """
    供应商熔断状态。
    """

    CLOSED = "closed"  # 正常工作，放行请求。
    OPEN = "open"  # 冷却期内拒绝请求。
    HALF_OPEN = "half_open"  # 冷却结束后只放行单个恢复探测。


@dataclass(frozen=True)
class CircuitBreakerConfig:
    """
    熔断参数，默认值与 CC Switch 保持一致。

    :param failure_threshold: 连续失败达到该次数后打开熔断器
    :param success_threshold: 半开时连续成功达到该次数后恢复正常
    :param timeout_seconds: 打开后等待恢复探测的秒数，允许为零
    :param error_rate_threshold: 触发熔断的失败比例，范围为零至一
    :param min_requests: 计算失败比例之前至少需要的请求数
    """

    failure_threshold: int = 4
    success_threshold: int = 2
    timeout_seconds: float = 60
    error_rate_threshold: float = 0.6
    min_requests: int = 10

    def __post_init__(self) -> None:
        """
        拒绝会使熔断器无法恢复或无法判断失败比例的参数。

        :raises ValueError: 阈值或冷却时间不在允许范围内
        """
        if self.failure_threshold < 1 or self.success_threshold < 1 or self.min_requests < 1:
            raise ValueError("熔断次数阈值必须为正数")
        if self.timeout_seconds < 0 or not 0 <= self.error_rate_threshold <= 1:
            raise ValueError("熔断冷却时间或失败比例不在允许范围内")


@dataclass(frozen=True)
class AllowResult:
    """
    请求许可结果，半开许可必须随结果归还。

    :param allowed: 是否允许实际发送请求
    :param used_half_open_permit: 是否占用了唯一的半开探测许可
    """

    allowed: bool
    used_half_open_permit: bool = False


@dataclass(frozen=True)
class CircuitBreakerStats:
    """
    同一把锁保护下读取的健康统计快照。

    :param state: 当前熔断状态，不主动触发超时状态转换
    :param consecutive_failures: 本轮连续失败次数，打开时归零
    :param consecutive_successes: 半开状态的连续成功次数
    :param total_requests: 正常关闭以来累计完成的请求数
    :param failed_requests: 正常关闭以来累计失败的请求数
    :param half_open_requests: 正在使用的半开探测许可数，最多为一
    """

    state: CircuitState
    consecutive_failures: int
    consecutive_successes: int
    total_requests: int
    failed_requests: int
    half_open_requests: int


class CircuitBreaker:
    """
    使用互斥锁保护的熔断器，可在多个 MoviePilot 后台线程之间共享。

    :ivar _config: 当前熔断阈值，热更新时保留已有健康统计
    :ivar _clock: 单调时钟，用于冷却计时且不受系统时间调整影响
    """

    def __init__(
        self,
        config: CircuitBreakerConfig | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """
        创建处于正常状态的熔断器。

        :param config: 熔断参数，省略时使用上游默认值
        :param clock: 返回单调秒数的时钟，测试可注入可推进的时钟
        """
        self._lock = threading.RLock()
        self._config = config or CircuitBreakerConfig()
        self._clock = clock
        self._state = CircuitState.CLOSED
        self._last_opened_at: float | None = None
        self._consecutive_failures = 0
        self._consecutive_successes = 0
        self._total_requests = 0
        self._failed_requests = 0
        self._half_open_requests = 0

    def update_config(self, config: CircuitBreakerConfig) -> None:
        """
        热更新参数且不重置状态或已持有的许可。

        :param config: 已验证的熔断参数
        """
        with self._lock:
            self._config = config

    def _refresh_availability(self) -> None:
        """
        在锁内将冷却结束的打开状态转换为半开状态。
        """
        if (
            self._state == CircuitState.OPEN
            and self._last_opened_at is not None
            and self._clock() - self._last_opened_at >= self._config.timeout_seconds
        ):
            self._state = CircuitState.HALF_OPEN
            self._consecutive_successes = 0
            # 已有半开请求仍在完成时保留许可，避免热更新或旧请求释放造成重复放行。

    def is_available(self) -> bool:
        """
        判断能否纳入候选队列，不占用半开许可。

        实際请求发出前仍须调用 allow_request；半开时多个候选检查均可返回真。

        :return: 当前正常或已进入恢复探测状态时返回真
        """
        with self._lock:
            self._refresh_availability()
            return self._state != CircuitState.OPEN

    def allow_request(self) -> AllowResult:
        """
        原子获取请求许可，半开状态最多放行一个请求。

        :return: 是否放行以及本次是否占用了半开许可
        """
        with self._lock:
            self._refresh_availability()
            if self._state == CircuitState.CLOSED:
                return AllowResult(True)
            if self._state == CircuitState.HALF_OPEN and not self._half_open_requests:
                self._half_open_requests = 1
                return AllowResult(True, True)
            return AllowResult(False)

    def _release_permit(self, used_half_open_permit: bool) -> None:
        """
        在锁内归还半开许可，重复释放不会使计数变为负数。
        """
        if used_half_open_permit and self._half_open_requests:
            self._half_open_requests -= 1

    def record_success(self, used_half_open_permit: bool = False) -> None:
        """
        记录一次成功并归还许可，半开达到成功阈值后恢复正常。

        :param used_half_open_permit: 本次请求取得的许可结果中的同名字段
        """
        with self._lock:
            self._release_permit(used_half_open_permit)
            self._consecutive_failures = 0
            self._total_requests += 1
            if self._state == CircuitState.HALF_OPEN:
                self._consecutive_successes += 1
                if self._consecutive_successes >= self._config.success_threshold:
                    self._transition_to_closed()

    def record_failure(self, used_half_open_permit: bool = False) -> None:
        """
        记录一次失败并归还许可，半开失败时立即重新打开熔断器。

        :param used_half_open_permit: 本次请求取得的许可结果中的同名字段
        """
        with self._lock:
            self._release_permit(used_half_open_permit)
            self._consecutive_failures += 1
            self._consecutive_successes = 0
            self._total_requests += 1
            self._failed_requests += 1
            if self._state == CircuitState.HALF_OPEN:
                self._transition_to_open()
            elif self._state == CircuitState.CLOSED:
                enough_failures = self._consecutive_failures >= self._config.failure_threshold
                excessive_error_rate = (
                    self._total_requests >= self._config.min_requests
                    and self._failed_requests / self._total_requests >= self._config.error_rate_threshold
                )
                if enough_failures or excessive_error_rate:
                    self._transition_to_open()

    def release_permit_neutral(self, used_half_open_permit: bool = False) -> None:
        """
        取消或无关错误只归还许可，不改变健康统计及状态。

        :param used_half_open_permit: 被取消请求取得的半开许可标记
        """
        with self._lock:
            self._release_permit(used_half_open_permit)

    def _transition_to_open(self) -> None:
        """
        在锁内开始新一轮冷却，保留累计请求统计。
        """
        self._state = CircuitState.OPEN
        self._last_opened_at = self._clock()
        self._consecutive_failures = 0
        self._consecutive_successes = 0

    def _transition_to_closed(self) -> None:
        """
        在锁内恢复正常并清除健康统计，保留尚未归还的许可。
        """
        self._state = CircuitState.CLOSED
        self._last_opened_at = None
        self._consecutive_failures = 0
        self._consecutive_successes = 0
        self._total_requests = 0
        self._failed_requests = 0

    def reset(self) -> None:
        """
        手动恢复正常并清除统计，不撤销仍在使用的半开许可。
        """
        with self._lock:
            self._transition_to_closed()

    def get_state(self) -> CircuitState:
        """
        读取当前状态，不触发超时转换。

        :return: 当前熔断状态
        """
        with self._lock:
            return self._state

    def get_stats(self) -> CircuitBreakerStats:
        """
        读取原子的健康统计快照。

        :return: 当前状态、结果统计及半开许可数
        """
        with self._lock:
            return CircuitBreakerStats(
                state=self._state,
                consecutive_failures=self._consecutive_failures,
                consecutive_successes=self._consecutive_successes,
                total_requests=self._total_requests,
                failed_requests=self._failed_requests,
                half_open_requests=self._half_open_requests,
            )
