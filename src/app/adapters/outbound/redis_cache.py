import redis
import structlog

logger = structlog.get_logger(__name__)


class RedisCache:
    """Cache fail-open: se o Redis estiver fora, opera como cache vazio.

    O cache é otimização de leitura — nunca pode derrubar o fluxo principal.
    """

    def __init__(self, client) -> None:
        self._client = client

    @classmethod
    def from_url(cls, url: str) -> "RedisCache":
        return cls(redis.Redis.from_url(url, decode_responses=True, socket_timeout=0.5))

    def get(self, key: str) -> str | None:
        try:
            return self._client.get(key)
        except Exception as exc:
            logger.warning("cache.get_failed", key=key, error=str(exc))
            return None

    def set(self, key: str, value: str, ttl_seconds: int) -> None:
        try:
            self._client.set(key, value, ex=ttl_seconds)
        except Exception as exc:
            logger.warning("cache.set_failed", key=key, error=str(exc))

    def delete(self, key: str) -> None:
        try:
            self._client.delete(key)
        except Exception as exc:
            logger.warning("cache.delete_failed", key=key, error=str(exc))
