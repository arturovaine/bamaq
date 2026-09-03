import os

from app.settings import Settings


def _clear_app_env(monkeypatch):
    for key in list(os.environ):
        if key.startswith("APP_"):
            monkeypatch.delenv(key)


def test_defaults(monkeypatch):
    _clear_app_env(monkeypatch)
    s = Settings(_env_file=None)
    assert s.database_url.startswith("mysql+pymysql://")
    assert s.kafka_topic == "transaction-events"
    assert s.kafka_retry_topic == "transaction-events.retry"
    assert s.kafka_dlq_topic == "transaction-events.dlq"
    assert s.max_processing_attempts == 5
    assert s.cache_ttl_seconds == 60


def test_env_vars_override_defaults(monkeypatch):
    _clear_app_env(monkeypatch)
    monkeypatch.setenv("APP_KAFKA_TOPIC", "outro-topico")
    monkeypatch.setenv("APP_CACHE_TTL_SECONDS", "5")
    s = Settings(_env_file=None)
    assert s.kafka_topic == "outro-topico"
    assert s.cache_ttl_seconds == 5
