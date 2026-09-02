"""Mock do serviço de análise de risco.

Regra determinística: value > REJECT_ABOVE -> REJECTED, senão APPROVED.
Modos de falha controláveis em runtime via POST /control:
  {"mode": "normal" | "fail", "latency_seconds": float}
"""
import os
import time
from decimal import Decimal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Mock Risk Analysis")

REJECT_ABOVE = Decimal(os.environ.get("RISK_REJECT_ABOVE", "10000"))


class State:
    mode: str = os.environ.get("RISK_MODE", "normal")
    latency_seconds: float = float(os.environ.get("RISK_LATENCY_SECONDS", "0"))


state = State()


class RiskRequest(BaseModel):
    customer_id: str
    value: float


class ControlRequest(BaseModel):
    mode: str | None = None
    latency_seconds: float | None = None


@app.post("/risk-analysis")
def analyze(body: RiskRequest):
    if state.latency_seconds:
        time.sleep(state.latency_seconds)
    if state.mode == "fail":
        raise HTTPException(status_code=503, detail="risk service unavailable")
    result = "REJECTED" if Decimal(str(body.value)) > REJECT_ABOVE else "APPROVED"
    return {"result": result}


@app.post("/control")
def control(body: ControlRequest):
    if body.mode is not None:
        state.mode = body.mode
    if body.latency_seconds is not None:
        state.latency_seconds = body.latency_seconds
    return {"mode": state.mode, "latency_seconds": state.latency_seconds}


@app.get("/health")
def health():
    return {"status": "ok"}
