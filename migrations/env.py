from alembic import context
from sqlalchemy import create_engine

from app.adapters.outbound.db import Base
from app.settings import Settings

target_metadata = Base.metadata


def run_migrations_online() -> None:
    engine = create_engine(Settings().database_url, pool_pre_ping=True)
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


run_migrations_online()
