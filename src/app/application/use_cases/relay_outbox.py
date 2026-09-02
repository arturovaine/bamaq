from collections.abc import Callable
from datetime import datetime

import structlog

from app.application.ports import EventPublisher, UnitOfWork
from app.clock import utcnow

logger = structlog.get_logger(__name__)


class RelayOutbox:
    """Publica eventos pendentes do outbox no Kafka (at-least-once).

    Se a publicação falhar, nada é marcado como publicado e o batch inteiro
    é retentado no próximo ciclo — duplicatas são absorvidas pelos consumers.
    """

    def __init__(
        self,
        uow_factory: Callable[[], UnitOfWork],
        publisher: EventPublisher,
        *,
        batch_size: int = 100,
        now: Callable[[], datetime] = utcnow,
    ) -> None:
        self._uow_factory = uow_factory
        self._publisher = publisher
        self._batch_size = batch_size
        self._now = now

    def execute_once(self) -> int:
        with self._uow_factory() as uow:
            records = uow.outbox.fetch_unpublished(self._batch_size)
            if not records:
                return 0
            for record in records:
                self._publisher.publish(key=record.aggregate_id, envelope=record.envelope())
            self._publisher.flush()
            published_at = self._now()
            for record in records:
                uow.outbox.mark_published(record.event_id, published_at)
            uow.commit()
        logger.info("outbox.relayed", count=len(records))
        return len(records)
