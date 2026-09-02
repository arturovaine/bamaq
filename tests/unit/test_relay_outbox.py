from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.application.use_cases.create_transaction import CreateTransaction
from app.application.use_cases.relay_outbox import RelayOutbox
from tests.unit.fakes import FakePublisher, FakeUnitOfWork

NOW = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)


def seed(uow, n):
    ids = iter([f"id-{i}" for i in range(2 * n)])
    uc = CreateTransaction(lambda: uow, new_id=lambda: next(ids), now=lambda: NOW)
    for _ in range(n):
        uc.execute(customer_id="123", value=Decimal("10.00"))


def test_publishes_and_marks_published():
    uow, publisher = FakeUnitOfWork(), FakePublisher()
    seed(uow, 2)
    relay = RelayOutbox(lambda: uow, publisher, now=lambda: NOW)
    assert relay.execute_once() == 2
    assert len(publisher.published) == 2
    key, envelope = publisher.published[0]
    assert key == "id-0"          # key = aggregate_id preserva ordem por transação
    assert envelope["event_type"] == "transaction.created"
    assert envelope["version"] == 1
    assert uow.outbox.fetch_unpublished(10) == []
    assert publisher.flushes == 1


def test_publish_failure_marks_nothing():
    uow = FakeUnitOfWork()
    seed(uow, 1)
    relay = RelayOutbox(lambda: uow, FakePublisher(fail=True), now=lambda: NOW)
    with pytest.raises(RuntimeError):
        relay.execute_once()
    assert len(uow.outbox.fetch_unpublished(10)) == 1  # nada marcado; será retentado


def test_empty_outbox_is_noop():
    assert RelayOutbox(lambda: FakeUnitOfWork(), FakePublisher()).execute_once() == 0
