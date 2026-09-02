from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.domain.transaction import InvalidTransition, Transaction, TransactionStatus

NOW = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)


def make_tx(status=TransactionStatus.PENDING):
    return Transaction(
        id="tx-1", customer_id="123", value=Decimal("1500.00"),
        status=status, created_at=NOW, updated_at=NOW,
    )


@pytest.mark.parametrize("start,target", [
    (TransactionStatus.PENDING, TransactionStatus.PROCESSING),
    (TransactionStatus.PENDING, TransactionStatus.FAILED),
    (TransactionStatus.PROCESSING, TransactionStatus.APPROVED),
    (TransactionStatus.PROCESSING, TransactionStatus.REJECTED),
    (TransactionStatus.PROCESSING, TransactionStatus.FAILED),
])
def test_valid_transitions(start, target):
    tx = make_tx(start)
    tx.transition_to(target, now=NOW)
    assert tx.status == target
    assert tx.updated_at == NOW


@pytest.mark.parametrize("start,target", [
    (TransactionStatus.APPROVED, TransactionStatus.PROCESSING),
    (TransactionStatus.REJECTED, TransactionStatus.PENDING),
    (TransactionStatus.FAILED, TransactionStatus.APPROVED),
    (TransactionStatus.PENDING, TransactionStatus.APPROVED),
])
def test_invalid_transitions_raise(start, target):
    with pytest.raises(InvalidTransition):
        make_tx(start).transition_to(target, now=NOW)


def test_terminal_statuses():
    assert not make_tx(TransactionStatus.PENDING).is_terminal
    assert not make_tx(TransactionStatus.PROCESSING).is_terminal
    assert make_tx(TransactionStatus.APPROVED).is_terminal
    assert make_tx(TransactionStatus.REJECTED).is_terminal
    assert make_tx(TransactionStatus.FAILED).is_terminal
