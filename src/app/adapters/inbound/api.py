import time
from datetime import datetime
from decimal import Decimal

import structlog
from fastapi import FastAPI, HTTPException, Request
from prometheus_client import make_asgi_app
from pydantic import BaseModel, Field, field_validator

from app.application.mappers import transaction_to_dict
from app.application.use_cases.create_transaction import CreateTransaction
from app.application.use_cases.get_transaction import GetTransaction
from app.metrics import TRANSACTIONS_CREATED

logger = structlog.get_logger(__name__)


class CreateTransactionRequest(BaseModel):
    customer_id: str = Field(min_length=1, max_length=64)
    value: Decimal = Field(gt=0, max_digits=12, decimal_places=2)

    @field_validator("value")
    @classmethod
    def quantize_to_cents(cls, v: Decimal) -> Decimal:
        return v.quantize(Decimal("0.01"))


class TransactionResponse(BaseModel):
    id: str
    customer_id: str
    value: str
    status: str
    created_at: datetime
    updated_at: datetime


def create_app(create_tx: CreateTransaction, get_tx: GetTransaction) -> FastAPI:
    app = FastAPI(title="BAMAQ Transactions", version="1.0.0")

    @app.middleware("http")
    async def access_log(request: Request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        logger.info(
            "http.request", method=request.method, path=request.url.path,
            status_code=response.status_code,
            duration_ms=round((time.perf_counter() - start) * 1000, 1),
        )
        return response

    @app.post("/transactions", status_code=202, response_model=TransactionResponse)
    def create_transaction(body: CreateTransactionRequest):
        tx = create_tx.execute(customer_id=body.customer_id, value=body.value)
        TRANSACTIONS_CREATED.inc()
        return transaction_to_dict(tx)

    @app.get("/transactions/{transaction_id}", response_model=TransactionResponse)
    def get_transaction(transaction_id: str):
        tx = get_tx.execute(transaction_id)
        if tx is None:
            raise HTTPException(status_code=404, detail="transaction not found")
        return transaction_to_dict(tx)

    @app.get("/health")
    def health():
        return {"status": "ok"}

    app.mount("/metrics", make_asgi_app())
    return app
