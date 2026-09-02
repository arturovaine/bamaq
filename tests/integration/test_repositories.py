import os
import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.adapters.outbound.db import Base
from app.adapters.outbound.uow import SqlAlchemyUnitOfWork
from app.domain.events import transaction_created
from app.domain.transaction import Transaction, TransactionStatus

pytestmark = pytest.mark.integration

DATABASE_URL = os.environ.get(
    "APP_DATABASE_URL", "mysql+pymysql://app:app@localhost:3306/transactions"
)


@pytest.fixture(scope="module")
def session_factory():
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    Base.metadata.create_all(engine)
    yield sessionmaker(engine)
    engine.dispose()


@pytest.fixture()
def uow_factory(session_factory):
    return lambda: SqlAlchemyUnitOfWork(session_factory)


def make_tx():
    now = datetime.now(UTC).replace(microsecond=0)
    return Transaction(
        id=str(uuid.uuid4()), customer_id="123", value=Decimal("1500.00"),
        status=TransactionStatus.PENDING, created_at=now, updated_at=now,
    )


def test_transaction_roundtrip(uow_factory):
    tx = make_tx()
    with uow_factory() as uow:
        uow.transactions.add(tx)
        uow.commit()
    with uow_factory() as uow:
        stored = uow.transactions.get(tx.id)
    assert stored == tx


def test_conditional_update_semantics(uow_factory):
    tx = make_tx()
    with uow_factory() as uow:
        uow.transactions.add(tx)
        uow.commit()
    now = datetime.now(UTC)
    with uow_factory() as uow:
        assert uow.transactions.update_status(
            tx.id, TransactionStatus.PENDING, TransactionStatus.PROCESSING, now
        ) is True
        # segunda tentativa com expected desatualizado falha (idempotência)
        assert uow.transactions.update_status(
            tx.id, TransactionStatus.PENDING, TransactionStatus.PROCESSING, now
        ) is False
        uow.commit()


def test_outbox_fetch_and_mark(uow_factory):
    tx = make_tx()
    event = transaction_created(
        tx, event_id=str(uuid.uuid4()), occurred_at=datetime.now(UTC)
    )
    with uow_factory() as uow:
        uow.outbox.add(event, aggregate_id=tx.id)
        uow.commit()
    with uow_factory() as uow:
        records = uow.outbox.fetch_unpublished(1000)
        assert event.event_id in [r.event_id for r in records]
        uow.outbox.mark_published(event.event_id, datetime.now(UTC))
        uow.commit()
    with uow_factory() as uow:
        assert event.event_id not in [
            r.event_id for r in uow.outbox.fetch_unpublished(1000)
        ]
