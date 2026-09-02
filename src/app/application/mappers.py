import json
from datetime import datetime
from decimal import Decimal

from app.domain.transaction import Transaction, TransactionStatus


def transaction_to_dict(tx: Transaction) -> dict:
    return {
        "id": tx.id,
        "customer_id": tx.customer_id,
        "value": str(tx.value),
        "status": tx.status.value,
        "created_at": tx.created_at.isoformat(),
        "updated_at": tx.updated_at.isoformat(),
    }


def transaction_from_dict(data: dict) -> Transaction:
    return Transaction(
        id=data["id"],
        customer_id=data["customer_id"],
        value=Decimal(data["value"]),
        status=TransactionStatus(data["status"]),
        created_at=datetime.fromisoformat(data["created_at"]),
        updated_at=datetime.fromisoformat(data["updated_at"]),
    )


def transaction_to_json(tx: Transaction) -> str:
    return json.dumps(transaction_to_dict(tx))


def transaction_from_json(raw: str) -> Transaction:
    return transaction_from_dict(json.loads(raw))
