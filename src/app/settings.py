from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="APP_", env_file=".env", extra="ignore")

    database_url: str = "mysql+pymysql://app:app@localhost:3306/transactions"
    kafka_bootstrap_servers: str = "localhost:29092"
    kafka_topic: str = "transaction-events"
    kafka_retry_topic: str = "transaction-events.retry"
    kafka_dlq_topic: str = "transaction-events.dlq"
    kafka_consumer_group: str = "transaction-processor"
    redis_url: str = "redis://localhost:6379/0"
    cache_ttl_seconds: int = 60
    risk_service_url: str = "http://localhost:8081"
    risk_timeout_seconds: float = 2.0
    risk_retry_attempts: int = 3
    circuit_failure_threshold: int = 5
    circuit_reset_timeout_seconds: float = 30.0
    max_processing_attempts: int = 5
    retry_backoff_base_seconds: float = 2.0
    outbox_poll_interval_seconds: float = 1.0
    outbox_batch_size: int = 100
