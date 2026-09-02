import signal
import time

import structlog
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.adapters.outbound.kafka_publisher import KafkaEventPublisher, make_producer
from app.adapters.outbound.uow import SqlAlchemyUnitOfWork
from app.application.use_cases.relay_outbox import RelayOutbox
from app.logging import configure_logging
from app.settings import Settings

logger = structlog.get_logger(__name__)


def main() -> None:
    configure_logging()
    settings = Settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True, pool_recycle=3600)
    session_factory = sessionmaker(engine)
    relay = RelayOutbox(
        lambda: SqlAlchemyUnitOfWork(session_factory),
        KafkaEventPublisher(
            make_producer(settings.kafka_bootstrap_servers), settings.kafka_topic
        ),
        batch_size=settings.outbox_batch_size,
    )
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    logger.info("outbox_relay.started")
    while running:
        try:
            count = relay.execute_once()
        except Exception:
            logger.exception("outbox_relay.cycle_failed")
            count = 0
        if count == 0:
            time.sleep(settings.outbox_poll_interval_seconds)


if __name__ == "__main__":
    main()
