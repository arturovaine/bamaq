import pytest

from app.adapters.outbound.circuit_breaker import (
    CircuitBreaker, CircuitOpenError, CircuitState,
)


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def boom():
    raise ValueError("falhou")


def make_breaker(clock):
    return CircuitBreaker(failure_threshold=3, reset_timeout=30.0, clock=clock)


def test_opens_after_threshold_failures():
    breaker = make_breaker(FakeClock())
    for _ in range(3):
        with pytest.raises(ValueError):
            breaker.call(boom)
    assert breaker.state is CircuitState.OPEN
    with pytest.raises(CircuitOpenError):
        breaker.call(lambda: "ok")


def test_half_open_success_closes():
    clock = FakeClock()
    breaker = make_breaker(clock)
    for _ in range(3):
        with pytest.raises(ValueError):
            breaker.call(boom)
    clock.t = 31.0
    assert breaker.call(lambda: "ok") == "ok"
    assert breaker.state is CircuitState.CLOSED


def test_half_open_failure_reopens():
    clock = FakeClock()
    breaker = make_breaker(clock)
    for _ in range(3):
        with pytest.raises(ValueError):
            breaker.call(boom)
    clock.t = 31.0
    with pytest.raises(ValueError):
        breaker.call(boom)
    assert breaker.state is CircuitState.OPEN
    with pytest.raises(CircuitOpenError):
        breaker.call(lambda: "ok")


def test_success_resets_failure_count():
    breaker = make_breaker(FakeClock())
    for _ in range(2):
        with pytest.raises(ValueError):
            breaker.call(boom)
    breaker.call(lambda: "ok")
    for _ in range(2):
        with pytest.raises(ValueError):
            breaker.call(boom)
    assert breaker.state is CircuitState.CLOSED
