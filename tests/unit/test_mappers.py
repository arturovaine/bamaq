from datetime import UTC, datetime
from decimal import Decimal

from app.application.mappers import transaction_from_dict, transaction_to_dict
from app.domain.transaction import Transaction, TransactionStatus

NOW = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)


def test_roundtrip():
    tx = Transaction(
        id="tx-1", customer_id="123", value=Decimal("1500.00"),
        status=TransactionStatus.APPROVED, created_at=NOW, updated_at=NOW,
    )
    assert transaction_from_dict(transaction_to_dict(tx)) == tx
