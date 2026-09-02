FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .

COPY mock_risk_service ./mock_risk_service
COPY migrations ./migrations
COPY alembic.ini ./

CMD ["uvicorn", "app.entrypoints.api:app", "--host", "0.0.0.0", "--port", "8000"]
