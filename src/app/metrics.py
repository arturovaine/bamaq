from prometheus_client import Counter, Histogram

TRANSACTIONS_CREATED = Counter(
    "transactions_created_total", "Transações criadas via API"
)
TRANSACTIONS_PROCESSED = Counter(
    "transactions_processed_total", "Mensagens processadas pelo consumer", ["outcome"]
)
RISK_ANALYSIS_SECONDS = Histogram(
    "risk_analysis_seconds", "Latência da chamada ao serviço de análise de risco"
)
