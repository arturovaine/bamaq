from dataclasses import replace
from datetime import datetime
from decimal import Decimal

from app.application.ports import OutboxRecord, RiskDecision
from app.domain.events import DomainEvent
from app.domain.transaction import Transaction, TransactionStatus


class FakeTransactionRepository:
    def __init__(self) -> None:
        self.items: dict[str, Transaction] = {}

    def add(self, tx: Transaction) -> None:
        self.items[tx.id] = replace(tx)

    def get(self, tx_id: str) -> Transaction | None:
        tx = self.items.get(tx_id)
        return replace(tx) if tx else None

    def update_status(
        self, tx_id: str, expected: TransactionStatus,
        new: TransactionStatus, updated_at: datetime,
    ) -> bool:
        tx = self.items.get(tx_id)
        if tx is None or tx.status != expected:
            return False
        tx.status = new
        tx.updated_at = updated_at
        return True


class FakeOutboxRepository:
    def __init__(self) -> None:
        self.records: list[OutboxRecord] = []
        self.published: dict[str, datetime] = {}

    def add(self, event: DomainEvent, aggregate_id: str) -> None:
        self.records.append(OutboxRecord(
            event_id=event.event_id, aggregate_id=aggregate_id,
            event_type=event.event_type, version=event.version,
            payload=event.payload, occurred_at=event.occurred_at,
        ))

    def fetch_unpublished(self, limit: int) -> list[OutboxRecord]:
        pending = [r for r in self.records if r.event_id not in self.published]
        return pending[:limit]

    def mark_published(self, event_id: str, published_at: datetime) -> None:
        self.published[event_id] = published_at


class FakeUnitOfWork:
    def __init__(self) -> None:
        self.transactions = FakeTransactionRepository()
        self.outbox = FakeOutboxRepository()
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        return None

    def commit(self) -> None:
        self.commits += 1


class FakeCache:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.deleted: list[str] = []

    def get(self, key: str) -> str | None:
        return self.store.get(key)

    def set(self, key: str, value: str, ttl_seconds: int) -> None:
        self.store[key] = value

    def delete(self, key: str) -> None:
        self.store.pop(key, None)
        self.deleted.append(key)


class FakeRiskAnalyzer:
    """Devolve resultados/erros na ordem da lista `script`."""

    def __init__(self, script: list) -> None:
        self.script = list(script)
        self.calls: list[tuple[str, Decimal]] = []

    def analyze(self, customer_id: str, value: Decimal) -> RiskDecision:
        self.calls.append((customer_id, value))
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class FakePublisher:
    def __init__(self, fail: bool = False) -> None:
        self.published: list[tuple[str, dict]] = []
        self.fail = fail
        self.flushes = 0

    def publish(self, *, key: str, envelope: dict) -> None:
        if self.fail:
            raise RuntimeError("kafka indisponível")
        self.published.append((key, envelope))

    def flush(self) -> None:
        self.flushes += 1
