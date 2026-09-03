import json

import pytest

from app.adapters.outbound.kafka_publisher import (
    KafkaEventPublisher,
    KafkaMessageSink,
    make_producer,
)


class FakeProducer:
    def __init__(self, pending_after_flush=0):
        self.produced = []
        self.flush_calls = 0
        self._pending = pending_after_flush

    def produce(self, topic, key=None, value=None, headers=None):
        self.produced.append((topic, key, value, headers))

    def flush(self, timeout=None):
        self.flush_calls += 1
        return self._pending


def test_make_producer_enables_idempotence(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        "app.adapters.outbound.kafka_publisher.Producer",
        lambda config: captured.update(config),
    )
    make_producer("broker:9092")
    assert captured["bootstrap.servers"] == "broker:9092"
    assert captured["enable.idempotence"] is True
    assert captured["acks"] == "all"


def test_publisher_encodes_key_and_envelope():
    producer = FakeProducer()
    KafkaEventPublisher(producer, "topico").publish(key="tx-1", envelope={"a": 1})
    topic, key, value, _ = producer.produced[0]
    assert topic == "topico"
    assert key == b"tx-1"
    assert json.loads(value) == {"a": 1}


def test_publisher_flush_ok():
    producer = FakeProducer()
    KafkaEventPublisher(producer, "topico").flush()
    assert producer.flush_calls == 1


def test_publisher_incomplete_flush_raises():
    with pytest.raises(RuntimeError, match="flush incompleto"):
        KafkaEventPublisher(FakeProducer(pending_after_flush=3), "topico").flush()


def test_sink_sends_with_headers_and_flushes_synchronously():
    producer = FakeProducer()
    KafkaMessageSink(producer).send("t.retry", b"k", b"v", headers={"attempts": b"1"})
    assert producer.produced == [("t.retry", b"k", b"v", [("attempts", b"1")])]
    assert producer.flush_calls == 1


def test_sink_without_headers():
    producer = FakeProducer()
    KafkaMessageSink(producer).send("t.dlq", None, b"v")
    assert producer.produced[0][3] is None


def test_sink_incomplete_flush_raises():
    with pytest.raises(RuntimeError, match="flush incompleto"):
        KafkaMessageSink(FakeProducer(pending_after_flush=1)).send("t", None, b"v")
