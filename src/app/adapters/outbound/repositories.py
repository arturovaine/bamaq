from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.adapters.outbound.db import OutboxModel, TransactionModel
from app.application.ports import OutboxRecord
from app.clock import ensure_utc
from app.domain.events import DomainEvent
from app.domain.transaction import Transaction, TransactionStatus


def _naive_utc(dt: datetime) -> datetime:
    return ensure_utc(dt).replace(tzinfo=None)


class SqlAlchemyTransactionRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add(self, tx: Transaction) -> None:
        self._session.add(TransactionModel(
            id=tx.id, customer_id=tx.customer_id, value=tx.value,
            status=tx.status.value,
            created_at=_naive_utc(tx.created_at), updated_at=_naive_utc(tx.updated_at),
        ))

    def get(self, tx_id: str) -> Transaction | None:
        model = self._session.get(TransactionModel, tx_id)
        if model is None:
            return None
        return Transaction(
            id=model.id, customer_id=model.customer_id, value=model.value,
            status=TransactionStatus(model.status),
            created_at=model.created_at.replace(tzinfo=UTC),
            updated_at=model.updated_at.replace(tzinfo=UTC),
        )

    def update_status(
        self, tx_id: str, expected: TransactionStatus,
        new: TransactionStatus, updated_at: datetime,
    ) -> bool:
        result = self._session.execute(
            update(TransactionModel)
            .where(TransactionModel.id == tx_id, TransactionModel.status == expected.value)
            .values(status=new.value, updated_at=_naive_utc(updated_at))
        )
        return result.rowcount == 1


class SqlAlchemyOutboxRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add(self, event: DomainEvent, aggregate_id: str) -> None:
        self._session.add(OutboxModel(
            event_id=event.event_id, aggregate_id=aggregate_id,
            event_type=event.event_type, version=event.version,
            payload=event.payload, occurred_at=_naive_utc(event.occurred_at),
        ))

    def fetch_unpublished(self, limit: int) -> list[OutboxRecord]:
        stmt = (
            select(OutboxModel)
            .where(OutboxModel.published_at.is_(None))
            .order_by(OutboxModel.occurred_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return [
            OutboxRecord(
                event_id=m.event_id, aggregate_id=m.aggregate_id,
                event_type=m.event_type, version=m.version,
                payload=m.payload,
                occurred_at=m.occurred_at.replace(tzinfo=UTC),
            )
            for m in self._session.scalars(stmt)
        ]

    def mark_published(self, event_id: str, published_at: datetime) -> None:
        self._session.execute(
            update(OutboxModel)
            .where(OutboxModel.event_id == event_id)
            .values(published_at=_naive_utc(published_at))
        )
