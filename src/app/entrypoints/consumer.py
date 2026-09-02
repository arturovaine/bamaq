import signal

from confluent_kafka import Consumer
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.adapters.inbound.consumer import KafkaConsumerLoop, MessageHandler
from app.adapters.outbound.kafka_publisher import KafkaMessageSink, make_producer
from app.adapters.outbound.redis_cache import RedisCache
from app.adapters.outbound.risk_client import HttpRiskAnalyzer
from app.adapters.outbound.uow import SqlAlchemyUnitOfWork
from app.application.use_cases.mark_transaction_failed import MarkTransactionFailed
from app.application.use_cases.process_transaction import ProcessTransaction
from app.logging import configure_logging
from app.settings import Settings


def main() -> None:
    configure_logging()
    settings = Settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True, pool_recycle=3600)
    session_factory = sessionmaker(engine)

    def uow_factory() -> SqlAlchemyUnitOfWork:
        return SqlAlchemyUnitOfWork(session_factory)

    cache = RedisCache.from_url(settings.redis_url)
    handler = MessageHandler(
        process=ProcessTransaction(
            uow_factory, HttpRiskAnalyzer.from_settings(settings), cache
        ),
        mark_failed=MarkTransactionFailed(uow_factory, cache),
        sink=KafkaMessageSink(make_producer(settings.kafka_bootstrap_servers)),
        retry_topic=settings.kafka_retry_topic,
        dlq_topic=settings.kafka_dlq_topic,
        max_attempts=settings.max_processing_attempts,
        backoff_base_seconds=settings.retry_backoff_base_seconds,
    )
    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.kafka_consumer_group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    loop = KafkaConsumerLoop(consumer, handler)
    signal.signal(signal.SIGTERM, lambda *_: loop.stop())
    signal.signal(signal.SIGINT, lambda *_: loop.stop())
    loop.run([settings.kafka_topic, settings.kafka_retry_topic])


if __name__ == "__main__":
    main()
