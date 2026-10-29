from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings
from app.core.logging import get_logger
from soma_shared.db.session import get_db_session as get_db_write_session

logger = get_logger(__name__)

_reader_engine: AsyncEngine | None = None
_reader_sessionmaker: async_sessionmaker[AsyncSession] | None = None


async def init_reader_db() -> None:
    global _reader_engine, _reader_sessionmaker
    if _reader_engine is not None:
        return

    reader_dsn = settings.get_postgres_reader_dsn()
    writer_dsn = settings.get_postgres_writer_dsn() or settings.get_postgres_dsn()
    if not reader_dsn or reader_dsn == writer_dsn:
        logger.info("reader_db_disabled")
        return

    _reader_engine = create_async_engine(
        reader_dsn,
        echo=settings.db_echo,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=True,
    )
    _reader_sessionmaker = async_sessionmaker(
        bind=_reader_engine,
        expire_on_commit=False,
    )
    try:
        async with _reader_engine.connect() as conn:
            await conn.exec_driver_sql("SELECT 1")
    except Exception:
        await _reader_engine.dispose()
        _reader_engine = None
        _reader_sessionmaker = None
        logger.exception("reader_db_init_failed")
        raise
    logger.info("reader_db_initialized")


async def close_reader_db() -> None:
    global _reader_engine, _reader_sessionmaker
    if _reader_engine is not None:
        await _reader_engine.dispose()
        _reader_engine = None
        _reader_sessionmaker = None


async def get_db_read_session() -> AsyncGenerator[AsyncSession, None]:
    if _reader_sessionmaker is None:
        async for session in get_db_write_session():
            yield session
            return
    async with _reader_sessionmaker() as session:
        yield session

