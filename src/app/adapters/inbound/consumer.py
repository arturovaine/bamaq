import json
import time
from collections.abc import Callable

import structlog
from confluent_kafka import Consumer, TopicPartition

from app.application.errors import RiskAnalysisPermanentError, RiskAnalysisUnavailable
from app.domain.events import TRANSACTION_CREATED
from app.metrics import TRANSACTIONS_PROCESSED

logger = structlog.get_logger(__name__)

ATTEMPTS_HEADER = "attempts"
NOT_BEFORE_HEADER = "not_before"


class MessageHandler:
    """Processa uma mensagem já decodificada. Livre de dependências do Kafka
    (recebe bytes/headers), o que permite teste unitário puro."""

    def __init__(
        self, *, process, mark_failed, sink,
        retry_topic: str, dlq_topic: str,
        max_attempts: int, backoff_base_seconds: float,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._process = process
        self._mark_failed = mark_failed
        self._sink = sink
        self._retry_topic = retry_topic
        self._dlq_topic = dlq_topic
        self._max_attempts = max_attempts
        self._backoff_base = backoff_base_seconds
        self._now = now

    def handle(self, *, value: bytes, headers: dict[str, bytes], key: bytes | None) -> None:
        try:
            envelope = json.loads(value)
            event_type = envelope["event_type"]
            tx_id = envelope["payload"]["transaction_id"]
        except (ValueError, KeyError, TypeError):
            logger.error(
                "consumer.unparseable_message", raw=value[:200].decode(errors="replace")
            )
            self._sink.send(self._dlq_topic, key, value, dict(headers))
            TRANSACTIONS_PROCESSED.labels(outcome="unparseable").inc()
            return

        log = logger.bind(transaction_id=tx_id, event_type=event_type)
        if event_type != TRANSACTION_CREATED:
            return  # status_changed é destinado a outros sistemas

        attempts = int(headers.get(ATTEMPTS_HEADER, b"0"))
        try:
            outcome = self._process.execute(tx_id)
        except RiskAnalysisUnavailable as exc:
            self._retry_or_dead_letter(tx_id, key, value, attempts, str(exc), log)
        except RiskAnalysisPermanentError as exc:
            log.error("consumer.permanent_failure", error=str(exc))
            self._dead_letter(tx_id, key, value, attempts, str(exc))
        else:
            log.info("consumer.processed", outcome=str(outcome), attempts=attempts)
            TRANSACTIONS_PROCESSED.labels(outcome=str(outcome)).inc()

    def _retry_or_dead_letter(self, tx_id, key, value, attempts, reason, log) -> None:
        next_attempts = attempts + 1
        if next_attempts >= self._max_attempts:
            log.error("consumer.retries_exhausted", attempts=next_attempts, reason=reason)
            self._dead_letter(tx_id, key, value, next_attempts, reason)
            return
        not_before = self._now() + self._backoff_base * (2 ** attempts)
        self._sink.send(self._retry_topic, key, value, {
            ATTEMPTS_HEADER: str(next_attempts).encode(),
            NOT_BEFORE_HEADER: str(not_before).encode(),
        })
        log.warning("consumer.retry_scheduled", attempts=next_attempts,
                    not_before=not_before, reason=reason)
        TRANSACTIONS_PROCESSED.labels(outcome="retried").inc()

    def _dead_letter(self, tx_id, key, value, attempts, reason) -> None:
        self._sink.send(self._dlq_topic, key, value,
                        {ATTEMPTS_HEADER: str(attempts).encode()})
        self._mark_failed.execute(tx_id, reason=reason)
        TRANSACTIONS_PROCESSED.labels(outcome="dead_lettered").inc()


class KafkaConsumerLoop:
    """Loop de consumo: commit manual após processar; mensagens de retry
    prematuras pausam a partição até `not_before` (sem bloquear as demais)."""

    def __init__(
        self, consumer: Consumer, handler: MessageHandler,
        *, now: Callable[[], float] = time.time,
    ) -> None:
        self._consumer = consumer
        self._handler = handler
        self._now = now
        self._paused: dict[tuple[str, int], tuple[TopicPartition, float]] = {}
        self._running = True

    def stop(self) -> None:
        self._running = False

    def run(self, topics: list[str]) -> None:
        self._consumer.subscribe(topics)
        logger.info("consumer.started", topics=topics)
        try:
            while self._running:
                self._resume_due_partitions()
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error("consumer.kafka_error", error=str(msg.error()))
                    continue
                self._handle(msg)
        finally:
            self._consumer.close()

    def _handle(self, msg) -> None:
        headers = {k: v for k, v in (msg.headers() or [])}
        not_before = headers.get(NOT_BEFORE_HEADER)
        if not_before is not None and float(not_before) > self._now():
            tp = TopicPartition(msg.topic(), msg.partition(), msg.offset())
            self._consumer.pause([tp])
            self._consumer.seek(tp)  # volta o offset p/ reentregar após resume
            self._paused[(msg.topic(), msg.partition())] = (tp, float(not_before))
            logger.info("consumer.partition_paused", topic=msg.topic(),
                        partition=msg.partition(), resume_at=float(not_before))
            return
        self._handler.handle(value=msg.value(), headers=headers, key=msg.key())
        self._consumer.commit(message=msg, asynchronous=False)

    def _resume_due_partitions(self) -> None:
        now = self._now()
        due = [k for k, (_, resume_at) in self._paused.items() if resume_at <= now]
        for k in due:
            tp, _ = self._paused.pop(k)
            self._consumer.resume([TopicPartition(tp.topic, tp.partition)])
            logger.info("consumer.partition_resumed", topic=tp.topic, partition=tp.partition)
