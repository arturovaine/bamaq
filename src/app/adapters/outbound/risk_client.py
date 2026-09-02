from decimal import Decimal

import httpx
import structlog
from tenacity import (
    retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter,
)

from app.adapters.outbound.circuit_breaker import CircuitBreaker, CircuitOpenError
from app.application.errors import RiskAnalysisPermanentError, RiskAnalysisUnavailable
from app.application.ports import RiskDecision
from app.metrics import RISK_ANALYSIS_SECONDS

logger = structlog.get_logger(__name__)


class HttpRiskAnalyzer:
    """Cliente do serviço de análise de risco.

    Camadas (de fora para dentro): circuit breaker -> retry (tenacity) -> HTTP.
    O breaker conta um ciclo completo de retries como uma falha única.
    """

    def __init__(
        self,
        *,
        client: httpx.Client,
        breaker: CircuitBreaker,
        retry_attempts: int = 3,
        retry_wait_seconds: float = 0.5,
    ) -> None:
        self._client = client
        self._breaker = breaker
        self._retry = retry(
            retry=retry_if_exception_type(RiskAnalysisUnavailable),
            stop=stop_after_attempt(retry_attempts),
            wait=wait_exponential_jitter(initial=retry_wait_seconds, max=5),
            reraise=True,
        )(self._request)

    @classmethod
    def from_settings(cls, settings) -> "HttpRiskAnalyzer":
        return cls(
            client=httpx.Client(
                base_url=settings.risk_service_url,
                timeout=settings.risk_timeout_seconds,
            ),
            breaker=CircuitBreaker(
                failure_threshold=settings.circuit_failure_threshold,
                reset_timeout=settings.circuit_reset_timeout_seconds,
            ),
            retry_attempts=settings.risk_retry_attempts,
        )

    def analyze(self, customer_id: str, value: Decimal) -> RiskDecision:
        try:
            return self._breaker.call(self._retry, customer_id, value)
        except CircuitOpenError as exc:
            raise RiskAnalysisUnavailable("circuit breaker aberto") from exc

    def _request(self, customer_id: str, value: Decimal) -> RiskDecision:
        try:
            with RISK_ANALYSIS_SECONDS.time():
                response = self._client.post(
                    "/risk-analysis",
                    json={"customer_id": customer_id, "value": float(value)},
                )
        except httpx.HTTPError as exc:
            logger.warning("risk.request_failed", error=str(exc))
            raise RiskAnalysisUnavailable(str(exc)) from exc
        if response.status_code >= 500:
            logger.warning("risk.server_error", status_code=response.status_code)
            raise RiskAnalysisUnavailable(f"status {response.status_code}")
        if response.status_code >= 400:
            raise RiskAnalysisPermanentError(f"status {response.status_code}")
        try:
            return RiskDecision(response.json()["result"])
        except (KeyError, ValueError) as exc:
            raise RiskAnalysisPermanentError(f"resposta inválida: {exc}") from exc
