from sqlalchemy.orm import Session, sessionmaker

from app.adapters.outbound.repositories import (
    SqlAlchemyOutboxRepository, SqlAlchemyTransactionRepository,
)


class SqlAlchemyUnitOfWork:
    def __init__(self, session_factory: sessionmaker) -> None:
        self._session_factory = session_factory
        self._session: Session | None = None

    def __enter__(self):
        self._session = self._session_factory()
        self.transactions = SqlAlchemyTransactionRepository(self._session)
        self.outbox = SqlAlchemyOutboxRepository(self._session)
        return self

    def __exit__(self, *exc) -> None:
        assert self._session is not None
        self._session.rollback()  # noop se já commitado
        self._session.close()
        self._session = None

    def commit(self) -> None:
        assert self._session is not None
        self._session.commit()
