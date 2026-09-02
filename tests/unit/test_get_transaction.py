from datetime import UTC, datetime
from decimal import Decimal

from app.application.mappers import transaction_to_json
from app.application.use_cases.get_transaction import CACHE_KEY_PREFIX, GetTransaction
from app.domain.transaction import Transaction, TransactionStatus
from tests.unit.fakes import FakeCache, FakeUnitOfWork

NOW = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
TX = Transaction(
    id="tx-1", customer_id="123", value=Decimal("1500.00"),
    status=TransactionStatus.APPROVED, created_at=NOW, updated_at=NOW,
)


def test_cache_hit_skips_database():
    uow, cache = FakeUnitOfWork(), FakeCache()
    cache.set(f"{CACHE_KEY_PREFIX}tx-1", transaction_to_json(TX), 60)
    result = GetTransaction(lambda: uow, cache).execute("tx-1")
    assert result == TX  # uow vazio: só pode ter vindo do cache


def test_cache_miss_reads_db_and_populates_cache():
    uow, cache = FakeUnitOfWork(), FakeCache()
    uow.transactions.add(TX)
    result = GetTransaction(lambda: uow, cache).execute("tx-1")
    assert result == TX
    assert f"{CACHE_KEY_PREFIX}tx-1" in cache.store


def test_missing_transaction_returns_none():
    assert GetTransaction(lambda: FakeUnitOfWork(), FakeCache()).execute("nope") is None
