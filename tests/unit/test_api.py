from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from app.adapters.inbound.api import create_app
from app.application.use_cases.create_transaction import CreateTransaction
from app.application.use_cases.get_transaction import GetTransaction
from tests.unit.fakes import FakeCache, FakeUnitOfWork

NOW = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)


@pytest.fixture()
def client():
    uow = FakeUnitOfWork()
    ids = iter([f"id-{i}" for i in range(100)])
    create = CreateTransaction(lambda: uow, new_id=lambda: next(ids), now=lambda: NOW)
    get = GetTransaction(lambda: uow, FakeCache())
    return TestClient(create_app(create, get))


def test_create_transaction_returns_202(client):
    resp = client.post("/transactions", json={"customer_id": "123", "value": 1500.00})
    assert resp.status_code == 202
    body = resp.json()
    assert body["id"] == "id-0"
    assert body["status"] == "PENDING"
    assert body["customer_id"] == "123"


def test_create_then_get(client):
    tx_id = client.post(
        "/transactions", json={"customer_id": "123", "value": 1500.00}
    ).json()["id"]
    resp = client.get(f"/transactions/{tx_id}")
    assert resp.status_code == 200
    assert resp.json()["value"] == "1500.00"


def test_get_unknown_returns_404(client):
    resp = client.get("/transactions/inexistente")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "transaction not found"


@pytest.mark.parametrize("payload", [
    {"customer_id": "", "value": 10},
    {"customer_id": "123", "value": -1},
    {"customer_id": "123", "value": 0},
    {"customer_id": "123"},
    {"value": 10},
    {"customer_id": "123", "value": 10.123},
])
def test_invalid_payload_returns_422(client, payload):
    assert client.post("/transactions", json=payload).status_code == 422


def test_health(client):
    assert client.get("/health").status_code == 200
