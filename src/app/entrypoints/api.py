from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.adapters.inbound.api import create_app
from app.adapters.outbound.redis_cache import RedisCache
from app.adapters.outbound.uow import SqlAlchemyUnitOfWork
from app.application.use_cases.create_transaction import CreateTransaction
from app.application.use_cases.get_transaction import GetTransaction
from app.logging import configure_logging
from app.settings import Settings

configure_logging()
settings = Settings()
engine = create_engine(settings.database_url, pool_pre_ping=True, pool_recycle=3600)
session_factory = sessionmaker(engine)


def uow_factory() -> SqlAlchemyUnitOfWork:
    return SqlAlchemyUnitOfWork(session_factory)


cache = RedisCache.from_url(settings.redis_url)
app = create_app(
    CreateTransaction(uow_factory),
    GetTransaction(uow_factory, cache, settings.cache_ttl_seconds),
)
