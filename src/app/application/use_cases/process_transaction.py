import uuid
from collections.abc import Callable
from datetime import datetime
from enum import StrEnum, auto

import structlog

from app.application.ports import CacheStore, RiskAnalyzer, RiskDecision, UnitOfWork
from app.application.use_cases.get_transaction import cache_key
from app.clock import utcnow
from app.domain.events import transaction_status_changed
from app.domain.transaction import TransactionStatus

logger = structlog.get_logger(__name__)


class ProcessOutcome(StrEnum):
    COMPLETED = auto()
    SKIPPED_ALREADY_DONE = auto()
    NOT_FOUND = auto()
    LOST_RACE = auto()


def _new_id() -> str:
    return str(uuid.uuid4())


class ProcessTransaction:
    def __init__(
        self,
        uow_factory: Callable[[], UnitOfWork],
        risk_analyzer: RiskAnalyzer,
        cache: CacheStore,
        *,
        new_id: Callable[[], str] = _new_id,
        now: Callable[[], datetime] = utcnow,
    ) -> None:
        self._uow_factory = uow_factory
        self._risk = risk_analyzer
        self._cache = cache
        self._new_id = new_id
        self._now = now

    def execute(self, tx_id: str) -> ProcessOutcome:
        log = logger.bind(transaction_id=tx_id)
        with self._uow_factory() as uow:
            tx = uow.transactions.get(tx_id)
            if tx is None:
                log.warning("process.transaction_not_found")
                return ProcessOutcome.NOT_FOUND
            if tx.is_terminal:
                log.info("process.skipped_already_done", status=tx.status)
                return ProcessOutcome.SKIPPED_ALREADY_DONE
            if tx.status == TransactionStatus.PENDING:
                uow.transactions.update_status(
                    tx_id, TransactionStatus.PENDING,
                    TransactionStatus.PROCESSING, self._now(),
                )
                uow.commit()

        # Chamada externa fora da transação de banco (pode demorar/falhar).
        decision = self._risk.analyze(tx.customer_id, tx.value)
        new_status = (
            TransactionStatus.APPROVED
            if decision is RiskDecision.APPROVED
            else TransactionStatus.REJECTED
        )

        with self._uow_factory() as uow:
            updated = uow.transactions.update_status(
                tx_id, TransactionStatus.PROCESSING, new_status, self._now()
            )
            if not updated:
                log.info("process.lost_race")
                return ProcessOutcome.LOST_RACE
            current = uow.transactions.get(tx_id)
            event = transaction_status_changed(
                current, old_status=TransactionStatus.PROCESSING,
                event_id=self._new_id(), occurred_at=self._now(),
            )
            uow.outbox.add(event, aggregate_id=tx_id)
            uow.commit()

        self._cache.delete(cache_key(tx_id))
        log.info("process.completed", status=new_status)
        return ProcessOutcome.COMPLETED
