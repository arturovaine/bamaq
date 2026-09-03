import structlog

from app.logging import configure_logging


def test_configure_logging_emits_json_with_context(capsys):
    configure_logging()
    structlog.get_logger("test_logging").info("evento_teste", transaction_id="tx-1")
    out = capsys.readouterr().out
    assert '"event": "evento_teste"' in out
    assert '"transaction_id": "tx-1"' in out
