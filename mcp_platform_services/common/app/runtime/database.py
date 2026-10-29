"""Connect to an already migrated database without running DDL in every worker.

SOMA-shared's init_db currently always creates tables and replaces views. Until
it exposes a connection-only initializer, keep the small session-registry adapter
here. All existing route/service imports still use the same SOMA-shared registry.
"""
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker
from soma_shared.db import session as shared_session


async def connect_existing_database(*, dsn, echo: bool, pool_size: int, max_overflow: int) -> None:
    if shared_session._engine is not None:
        return
    engine = shared_session.create_engine(
        dsn=dsn, echo=echo, pool_size=pool_size, max_overflow=max_overflow,
    )
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except BaseException:
        await engine.dispose()
        raise
    shared_session._engine = engine
    shared_session._sessionmaker = async_sessionmaker(bind=engine, expire_on_commit=False)
