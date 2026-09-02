import uuid
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal

import structlog

from app.application.ports import UnitOfWork
from app.clock import utcnow
from app.domain.events import transaction_created
from app.domain.transaction import Transaction, TransactionStatus

logger = structlog.get_logger(__name__)


def _new_id() -> str:
    return str(uuid.uuid4())


class CreateTransaction:
    def __init__(
        self,
        uow_factory: Callable[[], UnitOfWork],
        *,
        new_id: Callable[[], str] = _new_id,
        now: Callable[[], datetime] = utcnow,
    ) -> None:
        self._uow_factory = uow_factory
        self._new_id = new_id
        self._now = now

    def execute(self, *, customer_id: str, value: Decimal) -> Transaction:
        ts = self._now()
        tx = Transaction(
            id=self._new_id(), customer_id=customer_id, value=value,
            status=TransactionStatus.PENDING, created_at=ts, updated_at=ts,
        )
        event = transaction_created(tx, event_id=self._new_id(), occurred_at=ts)
        with self._uow_factory() as uow:
            uow.transactions.add(tx)
            uow.outbox.add(event, aggregate_id=tx.id)
            uow.commit()
        logger.info("transaction.created", transaction_id=tx.id, customer_id=customer_id)
        return tx
