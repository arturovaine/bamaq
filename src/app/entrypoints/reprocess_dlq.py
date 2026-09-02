"""Republica mensagens da DLQ no tópico principal, zerando as tentativas.

Uso: python -m app.entrypoints.reprocess_dlq [--max N]
"""
import argparse

import structlog
from confluent_kafka import Consumer, KafkaError

from app.adapters.outbound.kafka_publisher import KafkaMessageSink, make_producer
from app.logging import configure_logging
from app.settings import Settings

logger = structlog.get_logger(__name__)


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--max", type=int, default=1000)
    args = parser.parse_args()

    settings = Settings()
    sink = KafkaMessageSink(make_producer(settings.kafka_bootstrap_servers))
    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": f"{settings.kafka_consumer_group}-dlq-reprocessor",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([settings.kafka_dlq_topic])
    count = 0
    try:
        while count < args.max:
            msg = consumer.poll(5.0)
            if msg is None:
                break  # DLQ drenada
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                logger.error("dlq.kafka_error", error=str(msg.error()))
                continue
            # sem headers: attempts volta a 0 no fluxo normal
            sink.send(settings.kafka_topic, msg.key(), msg.value())
            consumer.commit(message=msg, asynchronous=False)
            count += 1
            logger.info("dlq.reprocessed", key=(msg.key() or b"").decode())
    finally:
        consumer.close()
    logger.info("dlq.done", reprocessed=count)


if __name__ == "__main__":
    main()
