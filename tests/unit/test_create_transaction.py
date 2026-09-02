from datetime import UTC, datetime
from decimal import Decimal

from app.application.use_cases.create_transaction import CreateTransaction
from app.domain.transaction import TransactionStatus
from tests.unit.fakes import FakeUnitOfWork

NOW = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)


def make_uc(uow):
    ids = iter(["tx-1", "ev-1"])
    return CreateTransaction(lambda: uow, new_id=lambda: next(ids), now=lambda: NOW)


def test_creates_pending_transaction_with_outbox_event():
    uow = FakeUnitOfWork()
    tx = make_uc(uow).execute(customer_id="123", value=Decimal("1500.00"))

    assert tx.id == "tx-1"
    assert tx.status == TransactionStatus.PENDING
    stored = uow.transactions.get("tx-1")
    assert stored is not None and stored.value == Decimal("1500.00")

    assert len(uow.outbox.records) == 1
    record = uow.outbox.records[0]
    assert record.event_type == "transaction.created"
    assert record.aggregate_id == "tx-1"
    assert record.payload["value"] == "1500.00"
    assert uow.commits == 1
