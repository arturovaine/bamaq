from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.application.errors import RiskAnalysisUnavailable
from app.application.ports import RiskDecision
from app.application.use_cases.get_transaction import cache_key
from app.application.use_cases.process_transaction import ProcessOutcome, ProcessTransaction
from app.domain.transaction import Transaction, TransactionStatus
from tests.unit.fakes import FakeCache, FakeRiskAnalyzer, FakeUnitOfWork

NOW = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)


def make_tx(status=TransactionStatus.PENDING):
    return Transaction(
        id="tx-1", customer_id="123", value=Decimal("1500.00"),
        status=status, created_at=NOW, updated_at=NOW,
    )


def make_uc(uow, analyzer, cache):
    ids = iter(["ev-1", "ev-2", "ev-3"])
    return ProcessTransaction(
        lambda: uow, analyzer, cache, new_id=lambda: next(ids), now=lambda: NOW
    )


@pytest.mark.parametrize("decision,expected", [
    (RiskDecision.APPROVED, TransactionStatus.APPROVED),
    (RiskDecision.REJECTED, TransactionStatus.REJECTED),
])
def test_success_updates_status_outbox_and_cache(decision, expected):
    uow, cache = FakeUnitOfWork(), FakeCache()
    uow.transactions.add(make_tx())
    outcome = make_uc(uow, FakeRiskAnalyzer([decision]), cache).execute("tx-1")

    assert outcome is ProcessOutcome.COMPLETED
    assert uow.transactions.get("tx-1").status == expected
    types = [r.event_type for r in uow.outbox.records]
    assert types == ["transaction.status_changed"]
    assert cache.deleted == [cache_key("tx-1")]


def test_terminal_transaction_is_skipped_idempotently():
    uow, cache = FakeUnitOfWork(), FakeCache()
    uow.transactions.add(make_tx(TransactionStatus.APPROVED))
    analyzer = FakeRiskAnalyzer([])
    outcome = make_uc(uow, analyzer, cache).execute("tx-1")
    assert outcome is ProcessOutcome.SKIPPED_ALREADY_DONE
    assert analyzer.calls == []  # serviço externo não é chamado de novo


def test_unknown_transaction():
    outcome = make_uc(FakeUnitOfWork(), FakeRiskAnalyzer([]), FakeCache()).execute("nope")
    assert outcome is ProcessOutcome.NOT_FOUND


def test_unavailable_service_bubbles_and_keeps_processing_status():
    uow, cache = FakeUnitOfWork(), FakeCache()
    uow.transactions.add(make_tx())
    uc = make_uc(uow, FakeRiskAnalyzer([RiskAnalysisUnavailable("timeout")]), cache)
    with pytest.raises(RiskAnalysisUnavailable):
        uc.execute("tx-1")
    assert uow.transactions.get("tx-1").status == TransactionStatus.PROCESSING


def test_redelivery_after_crash_processes_from_processing_state():
    # Cenário 2: banco foi atualizado p/ PROCESSING mas offset não foi commitado
    uow, cache = FakeUnitOfWork(), FakeCache()
    uow.transactions.add(make_tx(TransactionStatus.PROCESSING))
    outcome = make_uc(uow, FakeRiskAnalyzer([RiskDecision.APPROVED]), cache).execute("tx-1")
    assert outcome is ProcessOutcome.COMPLETED
    assert uow.transactions.get("tx-1").status == TransactionStatus.APPROVED
