from datetime import UTC, datetime
from decimal import Decimal

from app.application.use_cases.mark_transaction_failed import MarkTransactionFailed
from app.domain.transaction import Transaction, TransactionStatus
from tests.unit.fakes import FakeCache, FakeUnitOfWork

NOW = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)


def make_tx(status):
    return Transaction(
        id="tx-1", customer_id="123", value=Decimal("10.00"),
        status=status, created_at=NOW, updated_at=NOW,
    )


def make_uc(uow, cache):
    return MarkTransactionFailed(
        lambda: uow, cache, new_id=lambda: "ev-9", now=lambda: NOW
    )


def test_marks_failed_and_emits_event():
    uow, cache = FakeUnitOfWork(), FakeCache()
    uow.transactions.add(make_tx(TransactionStatus.PROCESSING))
    assert make_uc(uow, cache).execute("tx-1", reason="esgotou tentativas") is True
    assert uow.transactions.get("tx-1").status == TransactionStatus.FAILED
    assert uow.outbox.records[0].event_type == "transaction.status_changed"


def test_terminal_transaction_is_not_touched():
    uow, cache = FakeUnitOfWork(), FakeCache()
    uow.transactions.add(make_tx(TransactionStatus.APPROVED))
    assert make_uc(uow, cache).execute("tx-1", reason="x") is False
    assert uow.transactions.get("tx-1").status == TransactionStatus.APPROVED
