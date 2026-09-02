class RiskAnalysisUnavailable(Exception):
    """Falha transitória: timeout, 5xx, conexão recusada, circuito aberto."""


class RiskAnalysisPermanentError(Exception):
    """Falha permanente: resposta inválida ou 4xx. Não adianta reprocessar."""
