from dataclasses import dataclass
from datetime import datetime

from app.domain.transaction import Transaction, TransactionStatus

TRANSACTION_CREATED = "transaction.created"
TRANSACTION_STATUS_CHANGED = "transaction.status_changed"


@dataclass(frozen=True)
class DomainEvent:
    event_id: str
    event_type: str
    version: int
    occurred_at: datetime
    payload: dict


def _base_payload(tx: Transaction) -> dict:
    return {
        "transaction_id": tx.id,
        "customer_id": tx.customer_id,
        "value": str(tx.value),
        "status": tx.status.value,
    }


def transaction_created(tx: Transaction, *, event_id: str, occurred_at: datetime) -> DomainEvent:
    return DomainEvent(
        event_id=event_id, event_type=TRANSACTION_CREATED, version=1,
        occurred_at=occurred_at, payload=_base_payload(tx),
    )


def transaction_status_changed(
    tx: Transaction, *, old_status: TransactionStatus, event_id: str, occurred_at: datetime
) -> DomainEvent:
    payload = _base_payload(tx) | {"old_status": old_status.value}
    return DomainEvent(
        event_id=event_id, event_type=TRANSACTION_STATUS_CHANGED, version=1,
        occurred_at=occurred_at, payload=payload,
    )
