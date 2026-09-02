import json

from app.adapters.inbound.consumer import (
    ATTEMPTS_HEADER, NOT_BEFORE_HEADER, MessageHandler,
)
from app.application.errors import RiskAnalysisPermanentError, RiskAnalysisUnavailable
from app.application.use_cases.process_transaction import ProcessOutcome


class FakeProcess:
    def __init__(self, result=ProcessOutcome.COMPLETED, error=None):
        self.result, self.error, self.calls = result, error, []

    def execute(self, tx_id):
        self.calls.append(tx_id)
        if self.error:
            raise self.error
        return self.result


class FakeMarkFailed:
    def __init__(self):
        self.calls = []

    def execute(self, tx_id, *, reason):
        self.calls.append((tx_id, reason))
        return True


class FakeSink:
    def __init__(self):
        self.sent = []

    def send(self, topic, key, value, headers=None):
        self.sent.append((topic, key, value, headers))


def envelope(event_type="transaction.created", tx_id="tx-1"):
    return json.dumps({
        "event_id": "ev-1", "event_type": event_type, "version": 1,
        "occurred_at": "2026-09-01T12:00:00+00:00",
        "payload": {"transaction_id": tx_id, "customer_id": "123",
                    "value": "10.00", "status": "PENDING"},
    }).encode()


def make_handler(process, mark_failed=None, sink=None, max_attempts=3):
    return MessageHandler(
        process=process, mark_failed=mark_failed or FakeMarkFailed(),
        sink=sink or FakeSink(),
        retry_topic="t.retry", dlq_topic="t.dlq",
        max_attempts=max_attempts, backoff_base_seconds=2.0, now=lambda: 1000.0,
    )


def test_happy_path_processes_created_event():
    process = FakeProcess()
    make_handler(process).handle(value=envelope(), headers={}, key=b"tx-1")
    assert process.calls == ["tx-1"]


def test_status_changed_events_are_ignored():
    process = FakeProcess()
    make_handler(process).handle(
        value=envelope("transaction.status_changed"), headers={}, key=b"tx-1"
    )
    assert process.calls == []


def test_transient_failure_schedules_retry_with_backoff():
    sink = FakeSink()
    process = FakeProcess(error=RiskAnalysisUnavailable("down"))
    make_handler(process, sink=sink).handle(value=envelope(), headers={}, key=b"tx-1")
    topic, key, value, headers = sink.sent[0]
    assert topic == "t.retry"
    assert headers[ATTEMPTS_HEADER] == b"1"
    assert float(headers[NOT_BEFORE_HEADER]) == 1000.0 + 2.0  # base * 2**0


def test_retry_exhaustion_goes_to_dlq_and_marks_failed():
    sink, failed = FakeSink(), FakeMarkFailed()
    process = FakeProcess(error=RiskAnalysisUnavailable("down"))
    make_handler(process, mark_failed=failed, sink=sink, max_attempts=3).handle(
        value=envelope(), headers={ATTEMPTS_HEADER: b"2"}, key=b"tx-1"
    )
    assert sink.sent[0][0] == "t.dlq"
    assert failed.calls[0][0] == "tx-1"


def test_permanent_failure_goes_straight_to_dlq():
    sink, failed = FakeSink(), FakeMarkFailed()
    process = FakeProcess(error=RiskAnalysisPermanentError("422"))
    make_handler(process, mark_failed=failed, sink=sink).handle(
        value=envelope(), headers={}, key=b"tx-1"
    )
    assert sink.sent[0][0] == "t.dlq"
    assert failed.calls != []


def test_unparseable_message_goes_to_dlq():
    sink = FakeSink()
    make_handler(FakeProcess(), sink=sink).handle(
        value=b"nao-e-json", headers={}, key=None
    )
    assert sink.sent[0][0] == "t.dlq"
