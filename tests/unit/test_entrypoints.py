"""Wiring dos processos (entrypoints) com fakes — sem infraestrutura.

Engines/clients criados aqui são lazy (não conectam); Kafka e sinais são
substituídos por fakes via monkeypatch.
"""
import signal
import sys
from typing import ClassVar

from confluent_kafka import KafkaError

from app.adapters.outbound.uow import SqlAlchemyUnitOfWork


class FakeProducer:
    def __init__(self):
        self.produced = []

    def produce(self, topic, key=None, value=None, headers=None):
        self.produced.append((topic, key, value))

    def flush(self, timeout=None):
        return 0


def test_api_entrypoint_builds_app_and_uow():
    from app.entrypoints import api as entry

    routes = {route.path for route in entry.app.routes}
    assert {"/transactions", "/transactions/{transaction_id}", "/health"} <= routes
    assert isinstance(entry.uow_factory(), SqlAlchemyUnitOfWork)


def test_consumer_main_wires_signals_and_runs_loop(monkeypatch):
    from app.entrypoints import consumer as entry

    created, handlers = {}, {}

    class FakeLoop:
        def __init__(self, consumer, handler):
            self.topics, self.stopped = None, False
            created["loop"] = self

        def run(self, topics):
            self.topics = topics

        def stop(self):
            self.stopped = True

    monkeypatch.setattr(entry, "start_http_server", lambda port: created.setdefault("port", port))
    monkeypatch.setattr(entry, "make_producer", lambda servers: FakeProducer())
    monkeypatch.setattr(entry, "Consumer", lambda config: object())
    monkeypatch.setattr(entry, "KafkaConsumerLoop", FakeLoop)
    monkeypatch.setattr(entry.signal, "signal", lambda sig, h: handlers.__setitem__(sig, h))

    entry.main()

    loop = created["loop"]
    assert created["port"] == 9101
    assert len(loop.topics) == 2  # tópico principal + retry
    handlers[signal.SIGTERM](signal.SIGTERM, None)
    assert loop.stopped


def test_outbox_relay_main_publishes_logs_error_and_stops(monkeypatch):
    from app.entrypoints import outbox_relay as entry

    calls, handlers = {"n": 0}, {}

    class FakeRelay:
        def __init__(self, uow_factory, publisher, *, batch_size):
            pass

        def execute_once(self):
            calls["n"] += 1
            if calls["n"] == 1:
                return 1  # lote publicado: segue sem dormir
            raise RuntimeError("boom")  # ciclo com erro: loga e dorme

    monkeypatch.setattr(entry, "RelayOutbox", FakeRelay)
    monkeypatch.setattr(entry, "make_producer", lambda servers: FakeProducer())
    monkeypatch.setattr(entry.signal, "signal", lambda sig, h: handlers.__setitem__(sig, h))
    # o sleep do ciclo ocioso dispara o "SIGTERM" e encerra o loop
    monkeypatch.setattr(entry.time, "sleep", lambda seconds: handlers[signal.SIGTERM]())

    entry.main()
    assert calls["n"] == 2


class FakeDlqMsg:
    def __init__(self, key=b"tx-1", value=b"{}", error=None):
        self._key, self._value, self._error = key, value, error

    def key(self):
        return self._key

    def value(self):
        return self._value

    def error(self):
        return self._error


class FakeErr:
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code

    def __str__(self):
        return f"err({self._code})"


class FakeDlqConsumer:
    script: ClassVar[list] = []
    last: ClassVar["FakeDlqConsumer | None"] = None

    def __init__(self, config):
        self._messages = iter(type(self).script)
        self.commits, self.closed = 0, False
        type(self).last = self

    def subscribe(self, topics):
        self.topics = topics

    def poll(self, timeout):
        return next(self._messages, None)

    def commit(self, message, asynchronous):
        self.commits += 1

    def close(self):
        self.closed = True


def test_reprocess_dlq_republishes_and_drains(monkeypatch):
    from app.entrypoints import reprocess_dlq as entry

    producer = FakeProducer()
    FakeDlqConsumer.script = [
        FakeDlqMsg(error=FakeErr(1)),  # erro não-EOF: loga e continua
        FakeDlqMsg(),                  # mensagem válida: republica e commita
        None,                          # DLQ drenada: encerra
    ]
    monkeypatch.setattr(entry, "make_producer", lambda servers: producer)
    monkeypatch.setattr(entry, "Consumer", FakeDlqConsumer)
    monkeypatch.setattr(sys, "argv", ["reprocess_dlq"])

    entry.main()

    assert len(producer.produced) == 1
    assert FakeDlqConsumer.last.commits == 1
    assert FakeDlqConsumer.last.closed


def test_reprocess_dlq_stops_at_partition_eof(monkeypatch):
    from app.entrypoints import reprocess_dlq as entry

    producer = FakeProducer()
    FakeDlqConsumer.script = [FakeDlqMsg(error=FakeErr(KafkaError._PARTITION_EOF))]
    monkeypatch.setattr(entry, "make_producer", lambda servers: producer)
    monkeypatch.setattr(entry, "Consumer", FakeDlqConsumer)
    monkeypatch.setattr(sys, "argv", ["reprocess_dlq", "--max", "10"])

    entry.main()

    assert producer.produced == []
    assert FakeDlqConsumer.last.closed
