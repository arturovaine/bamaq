import json

from confluent_kafka import Producer


def make_producer(bootstrap_servers: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap_servers,
        "enable.idempotence": True,   # evita duplicatas do produtor em retries
        "acks": "all",
    })


class KafkaEventPublisher:
    """EventPublisher (port) para o tópico principal — usado pelo outbox relay."""

    def __init__(self, producer: Producer, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    def publish(self, *, key: str, envelope: dict) -> None:
        self._producer.produce(
            self._topic, key=key.encode(), value=json.dumps(envelope).encode()
        )

    def flush(self) -> None:
        remaining = self._producer.flush(timeout=10)
        if remaining:
            raise RuntimeError(f"kafka flush incompleto: {remaining} mensagens pendentes")


class KafkaMessageSink:
    """Envio direto a tópicos arbitrários (retry/DLQ) com flush síncrono.

    Flush por mensagem é aceitável: o caminho de retry/DLQ é de baixo volume
    e o offset só pode ser commitado após a durabilidade do reenvio.
    """

    def __init__(self, producer: Producer) -> None:
        self._producer = producer

    def send(
        self, topic: str, key: bytes | None, value: bytes,
        headers: dict[str, bytes] | None = None,
    ) -> None:
        self._producer.produce(
            topic, key=key, value=value,
            headers=list(headers.items()) if headers else None,
        )
        remaining = self._producer.flush(timeout=10)
        if remaining:
            raise RuntimeError(f"kafka flush incompleto: {remaining} mensagens pendentes")
