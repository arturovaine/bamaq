from decimal import Decimal

import httpx
import pytest

from app.adapters.outbound.circuit_breaker import CircuitBreaker
from app.adapters.outbound.risk_client import HttpRiskAnalyzer
from app.application.errors import RiskAnalysisPermanentError, RiskAnalysisUnavailable
from app.application.ports import RiskDecision


def make_analyzer(handler, breaker=None, attempts=3):
    client = httpx.Client(
        transport=httpx.MockTransport(handler), base_url="http://risk"
    )
    return HttpRiskAnalyzer(
        client=client,
        breaker=breaker or CircuitBreaker(failure_threshold=100, reset_timeout=1),
        retry_attempts=attempts,
        retry_wait_seconds=0.01,
    )


def test_approved():
    analyzer = make_analyzer(
        lambda req: httpx.Response(200, json={"result": "APPROVED"})
    )
    assert analyzer.analyze("123", Decimal("10")) is RiskDecision.APPROVED


def test_transient_5xx_is_retried_until_success():
    calls = []

    def handler(request):
        calls.append(request)
        if len(calls) < 3:
            return httpx.Response(503)
        return httpx.Response(200, json={"result": "REJECTED"})

    assert make_analyzer(handler).analyze("123", Decimal("10")) is RiskDecision.REJECTED
    assert len(calls) == 3


def test_exhausted_retries_raise_unavailable():
    def handler(request):
        raise httpx.ConnectError("down")

    with pytest.raises(RiskAnalysisUnavailable):
        make_analyzer(handler).analyze("123", Decimal("10"))


def test_4xx_is_permanent():
    analyzer = make_analyzer(lambda req: httpx.Response(422, json={}))
    with pytest.raises(RiskAnalysisPermanentError):
        analyzer.analyze("123", Decimal("10"))


def test_invalid_body_is_permanent():
    analyzer = make_analyzer(
        lambda req: httpx.Response(200, json={"result": "BANANA"})
    )
    with pytest.raises(RiskAnalysisPermanentError):
        analyzer.analyze("123", Decimal("10"))


def test_open_circuit_short_circuits_as_unavailable():
    breaker = CircuitBreaker(failure_threshold=1, reset_timeout=999)
    calls = []

    def handler(request):
        calls.append(request)
        raise httpx.ConnectError("down")

    analyzer = make_analyzer(handler, breaker=breaker, attempts=1)
    with pytest.raises(RiskAnalysisUnavailable):
        analyzer.analyze("123", Decimal("10"))
    n = len(calls)
    with pytest.raises(RiskAnalysisUnavailable):
        analyzer.analyze("123", Decimal("10"))
    assert len(calls) == n  # circuito aberto: serviço não foi chamado de novo
