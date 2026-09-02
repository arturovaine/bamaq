from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol, Self

from app.domain.events import DomainEvent
from app.domain.transaction import Transaction, TransactionStatus


class RiskDecision(StrEnum):
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


@dataclass
class OutboxRecord:
    event_id: str
    aggregate_id: str
    event_type: str
    version: int
    payload: dict
    occurred_at: datetime

    def envelope(self) -> dict:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "version": self.version,
            "occurred_at": self.occurred_at.isoformat(),
            "payload": self.payload,
        }


class TransactionRepository(Protocol):
    def add(self, tx: Transaction) -> None: ...
    def get(self, tx_id: str) -> Transaction | None: ...
    def update_status(
        self, tx_id: str, expected: TransactionStatus,
        new: TransactionStatus, updated_at: datetime,
    ) -> bool: ...


class OutboxRepository(Protocol):
    def add(self, event: DomainEvent, aggregate_id: str) -> None: ...
    def fetch_unpublished(self, limit: int) -> list[OutboxRecord]: ...
    def mark_published(self, event_id: str, published_at: datetime) -> None: ...


class UnitOfWork(Protocol):
    transactions: TransactionRepository
    outbox: OutboxRepository

    def __enter__(self) -> Self: ...
    def __exit__(self, *exc) -> None: ...
    def commit(self) -> None: ...


class RiskAnalyzer(Protocol):
    def analyze(self, customer_id: str, value: Decimal) -> RiskDecision: ...


class CacheStore(Protocol):
    def get(self, key: str) -> str | None: ...
    def set(self, key: str, value: str, ttl_seconds: int) -> None: ...
    def delete(self, key: str) -> None: ...


class EventPublisher(Protocol):
    def publish(self, *, key: str, envelope: dict) -> None: ...
    def flush(self) -> None: ...
