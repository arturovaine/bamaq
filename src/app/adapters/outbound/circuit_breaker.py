import time
from collections.abc import Callable
from enum import StrEnum, auto

import structlog

logger = structlog.get_logger(__name__)


class CircuitState(StrEnum):
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


class CircuitOpenError(Exception):
    """Circuito aberto: chamadas são rejeitadas sem tocar o serviço externo."""


class CircuitBreaker:
    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        reset_timeout: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._failure_threshold = failure_threshold
        self._reset_timeout = reset_timeout
        self._clock = clock
        self._failures = 0
        self._opened_at: float | None = None

    @property
    def state(self) -> CircuitState:
        if self._opened_at is None:
            return CircuitState.CLOSED
        if self._clock() - self._opened_at >= self._reset_timeout:
            return CircuitState.HALF_OPEN
        return CircuitState.OPEN

    def call(self, fn: Callable, *args, **kwargs):
        if self.state is CircuitState.OPEN:
            raise CircuitOpenError("circuit breaker aberto")
        try:
            result = fn(*args, **kwargs)
        except Exception:
            self._record_failure()
            raise
        self._reset()
        return result

    def _record_failure(self) -> None:
        was_half_open = self.state is CircuitState.HALF_OPEN
        self._failures += 1
        if self._failures >= self._failure_threshold or was_half_open:
            self._opened_at = self._clock()
            logger.warning("circuit_breaker.open", failures=self._failures)

    def _reset(self) -> None:
        if self._opened_at is not None:
            logger.info("circuit_breaker.closed")
        self._failures = 0
        self._opened_at = None
