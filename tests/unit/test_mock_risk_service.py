from fastapi.testclient import TestClient

from mock_risk_service.main import app, state


def client():
    state.mode = "normal"
    state.latency_seconds = 0.0
    return TestClient(app)


def test_low_value_is_approved():
    resp = client().post("/risk-analysis", json={"customer_id": "1", "value": 100.0})
    assert resp.status_code == 200
    assert resp.json() == {"result": "APPROVED"}


def test_high_value_is_rejected():
    resp = client().post("/risk-analysis", json={"customer_id": "1", "value": 99999.0})
    assert resp.json() == {"result": "REJECTED"}


def test_fail_mode_returns_503():
    c = client()
    c.post("/control", json={"mode": "fail"})
    assert c.post("/risk-analysis", json={"customer_id": "1", "value": 1.0}).status_code == 503
