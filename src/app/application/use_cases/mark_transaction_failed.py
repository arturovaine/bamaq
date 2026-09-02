import uuid
from collections.abc import Callable
from datetime import datetime

import structlog

from app.application.ports import CacheStore, UnitOfWork
from app.application.use_cases.get_transaction import cache_key
from app.clock import utcnow
from app.domain.events import transaction_status_changed
from app.domain.transaction import TransactionStatus

logger = structlog.get_logger(__name__)


def _new_id() -> str:
    return str(uuid.uuid4())


class MarkTransactionFailed:
    def __init__(
        self,
        uow_factory: Callable[[], UnitOfWork],
        cache: CacheStore,
        *,
        new_id: Callable[[], str] = _new_id,
        now: Callable[[], datetime] = utcnow,
    ) -> None:
        self._uow_factory = uow_factory
        self._cache = cache
        self._new_id = new_id
        self._now = now

    def execute(self, tx_id: str, *, reason: str) -> bool:
        log = logger.bind(transaction_id=tx_id)
        with self._uow_factory() as uow:
            tx = uow.transactions.get(tx_id)
            if tx is None or tx.is_terminal:
                return False
            updated = uow.transactions.update_status(
                tx_id, tx.status, TransactionStatus.FAILED, self._now()
            )
            if not updated:
                return False
            current = uow.transactions.get(tx_id)
            event = transaction_status_changed(
                current, old_status=tx.status,
                event_id=self._new_id(), occurred_at=self._now(),
            )
            uow.outbox.add(event, aggregate_id=tx_id)
            uow.commit()
        self._cache.delete(cache_key(tx_id))
        log.error("transaction.failed", reason=reason)
        return True
