"""Adapters SQLAlchemy contra SQLite em memória — sem infraestrutura.

Complementa tests/integration/test_repositories.py, que exercita os mesmos
adapters contra o MySQL real (onde FOR UPDATE SKIP LOCKED tem efeito).
"""
import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.adapters.outbound.db import Base
from app.adapters.outbound.uow import SqlAlchemyUnitOfWork
from app.domain.events import transaction_created
from app.domain.transaction import Transaction, TransactionStatus


@pytest.fixture()
def uow_factory():
    engine = create_engine(
        "sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(engine)
    yield lambda: SqlAlchemyUnitOfWork(factory)
    engine.dispose()


def make_tx():
    now = datetime.now(UTC).replace(microsecond=0)
    return Transaction(
        id=str(uuid.uuid4()), customer_id="123", value=Decimal("1500.00"),
        status=TransactionStatus.PENDING, created_at=now, updated_at=now,
    )


def test_transaction_roundtrip_and_missing(uow_factory):
    tx = make_tx()
    with uow_factory() as uow:
        uow.transactions.add(tx)
        uow.commit()
    with uow_factory() as uow:
        assert uow.transactions.get(tx.id) == tx
        assert uow.transactions.get("nao-existe") is None


def test_uncommitted_changes_are_rolled_back(uow_factory):
    tx = make_tx()
    with uow_factory() as uow:
        uow.transactions.add(tx)  # sem commit
    with uow_factory() as uow:
        assert uow.transactions.get(tx.id) is None


def test_conditional_update_has_single_winner(uow_factory):
    tx = make_tx()
    with uow_factory() as uow:
        uow.transactions.add(tx)
        uow.commit()
    now = datetime.now(UTC)
    with uow_factory() as uow:
        assert uow.transactions.update_status(
            tx.id, TransactionStatus.PENDING, TransactionStatus.PROCESSING, now
        ) is True
        assert uow.transactions.update_status(
            tx.id, TransactionStatus.PENDING, TransactionStatus.PROCESSING, now
        ) is False
        uow.commit()


def test_outbox_fetch_respects_limit_and_order(uow_factory):
    first, second = make_tx(), make_tx()
    with uow_factory() as uow:
        uow.outbox.add(
            transaction_created(
                first, event_id="e1", occurred_at=datetime(2026, 1, 1, tzinfo=UTC)
            ),
            aggregate_id=first.id,
        )
        uow.outbox.add(
            transaction_created(
                second, event_id="e2", occurred_at=datetime(2026, 1, 2, tzinfo=UTC)
            ),
            aggregate_id=second.id,
        )
        uow.commit()
    with uow_factory() as uow:
        records = uow.outbox.fetch_unpublished(1)
        assert [r.event_id for r in records] == ["e1"]  # mais antigo primeiro
        assert records[0].payload["transaction_id"] == first.id


def test_outbox_mark_published_excludes_from_fetch(uow_factory):
    tx = make_tx()
    event = transaction_created(tx, event_id="e1", occurred_at=datetime.now(UTC))
    with uow_factory() as uow:
        uow.outbox.add(event, aggregate_id=tx.id)
        uow.commit()
    with uow_factory() as uow:
        uow.outbox.mark_published("e1", datetime.now(UTC))
        uow.commit()
    with uow_factory() as uow:
        assert uow.outbox.fetch_unpublished(10) == []
