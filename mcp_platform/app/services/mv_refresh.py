from __future__ import annotations

import asyncio

from soma_shared.db.views.definitions import MV_DEFINITIONS
from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)


def _is_unpopulated_mv_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "materialized view" in message
        and "concurrently" in message
        and "not populated" in message
    )


def start_mv_refresh_task(app) -> None:
    task = asyncio.create_task(_run_refresh_loop())
    app.state.mv_refresh_task = task
    logger.info(
        "mv_refresh_started",
        extra={
            "views": [mv.name for mv in MV_DEFINITIONS],
            "default_interval_secs": settings.mv_refresh_interval_secs,
            "fast_interval_secs": settings.mv_refresh_fast_interval_secs,
        },
    )


async def stop_mv_refresh_task(app) -> None:
    task = getattr(app.state, "mv_refresh_task", None)
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    logger.info("mv_refresh_stopped")



async def _run_refresh_loop() -> None:
    interval = settings.mv_refresh_interval_secs

    # Track last refresh time per view (name → seconds since epoch float).
    import time

    last_refresh: dict[str, float] = {mv.name: 0.0 for mv in MV_DEFINITIONS}

    try:
        while True:
            now = time.monotonic()
            for mv in MV_DEFINITIONS:
                if now - last_refresh[mv.name] >= interval:
                    try:
                        async for conn in _get_raw_connection():
                            await conn.exec_driver_sql(
                                f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv.name}",
                                execution_options={"isolation_level": "AUTOCOMMIT"},
                            )
                        last_refresh[mv.name] = time.monotonic()
                        logger.info(f"mv_refreshed view: {mv.name}")
                    except Exception as exc:
                        if _is_unpopulated_mv_error(exc):
                            try:
                                async for conn in _get_raw_connection():
                                    await conn.exec_driver_sql(
                                        f"REFRESH MATERIALIZED VIEW {mv.name}",
                                        execution_options={
                                            "isolation_level": "AUTOCOMMIT"
                                        },
                                    )
                                last_refresh[mv.name] = time.monotonic()
                                logger.info(
                                    f"mv_refreshed_initial_nonconcurrent view: {mv.name}"
                                )
                            except Exception:
                                logger.exception(
                                    "mv_refresh_initial_failed",
                                    extra={"view": mv.name},
                                )
                        else:
                            logger.exception(
                                "mv_refresh_failed",
                                extra={"view": mv.name},
                            )

            # Sleep for the smallest interval so we can wake up in time
            # for the next fast view refresh.
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("mv_refresh_cancelled")
        raise


async def _get_raw_connection():
    """Yield a raw AsyncConnection via the public engine accessor."""
    from soma_shared.db.session import get_engine

    async with get_engine().connect() as conn:
        yield conn
        await conn.commit()
