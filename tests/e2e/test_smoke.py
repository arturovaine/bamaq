import time

import httpx
import pytest

pytestmark = pytest.mark.e2e

API = "http://localhost:8000"
RISK = "http://localhost:8081"


def wait_status(tx_id: str, expected: set[str], timeout: float = 30.0) -> str:
    deadline = time.time() + timeout
    status = None
    while time.time() < deadline:
        resp = httpx.get(f"{API}/transactions/{tx_id}")
        status = resp.json()["status"]
        if status in expected:
            return status
        time.sleep(0.5)
    raise AssertionError(f"timeout esperando {expected}; último status: {status}")


def test_happy_path_low_value_is_approved():
    resp = httpx.post(
        f"{API}/transactions", json={"customer_id": "42", "value": 1500.00}
    )
    assert resp.status_code == 202
    assert wait_status(resp.json()["id"], {"APPROVED"}) == "APPROVED"


def test_high_value_is_rejected():
    resp = httpx.post(
        f"{API}/transactions", json={"customer_id": "42", "value": 50000.00}
    )
    assert wait_status(resp.json()["id"], {"REJECTED"}) == "REJECTED"


def test_temporary_risk_outage_recovers():
    httpx.post(f"{RISK}/control", json={"mode": "fail"})
    try:
        resp = httpx.post(
            f"{API}/transactions", json={"customer_id": "42", "value": 10.00}
        )
        tx_id = resp.json()["id"]
        time.sleep(5)  # deixa acumular tentativas enquanto o serviço está fora
    finally:
        httpx.post(f"{RISK}/control", json={"mode": "normal"})
    assert wait_status(tx_id, {"APPROVED"}, timeout=90) == "APPROVED"


def test_unknown_transaction_is_404():
    assert httpx.get(f"{API}/transactions/nao-existe").status_code == 404
