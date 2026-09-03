.PHONY: up down logs test coverage test-integration test-e2e scale-consumers reprocess-dlq

up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f api consumer outbox-relay

test:
	.venv/bin/pytest tests/unit -v

coverage:
	.venv/bin/pytest tests/unit --cov=src --cov=mock_risk_service --cov-report=term-missing

test-integration:
	.venv/bin/pytest tests/integration -v -m integration

test-e2e:
	.venv/bin/pytest tests/e2e -v -m e2e

scale-consumers:
	docker compose up -d --scale consumer=3

reprocess-dlq:
	docker compose run --rm consumer python -m app.entrypoints.reprocess_dlq
