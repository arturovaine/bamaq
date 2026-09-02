from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum


class TransactionStatus(StrEnum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"


TERMINAL_STATUSES = frozenset(
    {TransactionStatus.APPROVED, TransactionStatus.REJECTED, TransactionStatus.FAILED}
)

VALID_TRANSITIONS: dict[TransactionStatus, frozenset[TransactionStatus]] = {
    TransactionStatus.PENDING: frozenset(
        {TransactionStatus.PROCESSING, TransactionStatus.FAILED}
    ),
    TransactionStatus.PROCESSING: frozenset(
        {TransactionStatus.APPROVED, TransactionStatus.REJECTED, TransactionStatus.FAILED}
    ),
    TransactionStatus.APPROVED: frozenset(),
    TransactionStatus.REJECTED: frozenset(),
    TransactionStatus.FAILED: frozenset(),
}


class InvalidTransition(Exception):
    def __init__(self, current: TransactionStatus, target: TransactionStatus) -> None:
        super().__init__(f"transição inválida: {current} -> {target}")
        self.current = current
        self.target = target


@dataclass
class Transaction:
    id: str
    customer_id: str
    value: Decimal
    status: TransactionStatus
    created_at: datetime
    updated_at: datetime

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    def transition_to(self, target: TransactionStatus, *, now: datetime) -> None:
        if target not in VALID_TRANSITIONS[self.status]:
            raise InvalidTransition(self.status, target)
        self.status = target
        self.updated_at = now
