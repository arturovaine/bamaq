from datetime import UTC, datetime
from decimal import Decimal

from app.domain.events import transaction_created, transaction_status_changed
from app.domain.transaction import Transaction, TransactionStatus

NOW = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
TX = Transaction(
    id="tx-1", customer_id="123", value=Decimal("1500.00"),
    status=TransactionStatus.PENDING, created_at=NOW, updated_at=NOW,
)


def test_transaction_created_envelope():
    ev = transaction_created(TX, event_id="ev-1", occurred_at=NOW)
    assert ev.event_id == "ev-1"
    assert ev.event_type == "transaction.created"
    assert ev.version == 1
    assert ev.occurred_at == NOW
    assert ev.payload == {
        "transaction_id": "tx-1",
        "customer_id": "123",
        "value": "1500.00",
        "status": "PENDING",
    }


def test_transaction_status_changed_envelope():
    ev = transaction_status_changed(
        TX, old_status=TransactionStatus.PROCESSING, event_id="ev-2", occurred_at=NOW
    )
    assert ev.event_type == "transaction.status_changed"
    assert ev.payload["old_status"] == "PROCESSING"
    assert ev.payload["status"] == "PENDING"
