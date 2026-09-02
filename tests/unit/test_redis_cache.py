import fakeredis

from app.adapters.outbound.redis_cache import RedisCache


class BrokenRedis:
    def get(self, key):
        raise ConnectionError("redis fora")

    def set(self, *a, **kw):
        raise ConnectionError("redis fora")

    def delete(self, key):
        raise ConnectionError("redis fora")


def make_cache():
    return RedisCache(client=fakeredis.FakeRedis(decode_responses=True))


def test_set_get_delete():
    cache = make_cache()
    cache.set("k", "v", ttl_seconds=60)
    assert cache.get("k") == "v"
    cache.delete("k")
    assert cache.get("k") is None


def test_redis_failure_is_swallowed():
    cache = RedisCache(client=BrokenRedis())
    cache.set("k", "v", ttl_seconds=60)   # não explode
    assert cache.get("k") is None         # trata como miss
    cache.delete("k")                     # não explode
