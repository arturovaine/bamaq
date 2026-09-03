from datetime import UTC, datetime, timedelta, timezone

from app.clock import ensure_utc, utcnow


def test_utcnow_is_utc_aware():
    assert utcnow().tzinfo == UTC


def test_ensure_utc_marks_naive_as_utc():
    dt = ensure_utc(datetime(2026, 1, 1, 12, 0))  # noqa: DTZ001 — naive é o caso testado
    assert dt.tzinfo == UTC
    assert dt.hour == 12


def test_ensure_utc_converts_aware_to_utc():
    brt = timezone(timedelta(hours=-3))
    dt = ensure_utc(datetime(2026, 1, 1, 9, 0, tzinfo=brt))
    assert dt.tzinfo == UTC
    assert dt.hour == 12
