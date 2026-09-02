from collections.abc import Callable

from app.application.mappers import transaction_from_json, transaction_to_json
from app.application.ports import CacheStore, UnitOfWork
from app.domain.transaction import Transaction

CACHE_KEY_PREFIX = "transaction:"


def cache_key(tx_id: str) -> str:
    return f"{CACHE_KEY_PREFIX}{tx_id}"


class GetTransaction:
    def __init__(
        self,
        uow_factory: Callable[[], UnitOfWork],
        cache: CacheStore,
        ttl_seconds: int = 60,
    ) -> None:
        self._uow_factory = uow_factory
        self._cache = cache
        self._ttl = ttl_seconds

    def execute(self, tx_id: str) -> Transaction | None:
        cached = self._cache.get(cache_key(tx_id))
        if cached is not None:
            return transaction_from_json(cached)
        with self._uow_factory() as uow:
            tx = uow.transactions.get(tx_id)
        if tx is not None:
            self._cache.set(cache_key(tx_id), transaction_to_json(tx), self._ttl)
        return tx
