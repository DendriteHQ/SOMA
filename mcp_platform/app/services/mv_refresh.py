from __future__ import annotations

import asyncio

from soma_shared.db.session import get_db_session
from soma_shared.db.views.definitions import MV_DEFINITIONS
from soma_shared.db.views.sync import refresh_materialized_views
from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)


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


# Views refreshed on the fast cadence (e.g. miner status / screener stats
# that are queried on every page load and change frequently).
_FAST_MV_NAMES: frozenset[str] = frozenset(
    {
        "mv_miner_status",
        "mv_miner_screener_stats",
        "mv_miner_competition_stats"
    }
)


async def _run_refresh_loop() -> None:
    fast_interval = settings.mv_refresh_fast_interval_secs
    slow_interval = settings.mv_refresh_interval_secs

    # Track last refresh time per view (name → seconds since epoch float).
    import time

    last_refresh: dict[str, float] = {mv.name: 0.0 for mv in MV_DEFINITIONS}

    try:
        while True:
            now = time.monotonic()
            for mv in MV_DEFINITIONS:
                interval = fast_interval if mv.name in _FAST_MV_NAMES else slow_interval
                if now - last_refresh[mv.name] >= interval:
                    try:
                        async for conn in _get_raw_connection():
                            await conn.exec_driver_sql(
                                f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv.name}",
                                execution_options={"isolation_level": "AUTOCOMMIT"},
                            )
                        last_refresh[mv.name] = time.monotonic()
                        logger.debug("mv_refreshed", extra={"view": mv.name})
                    except Exception:
                        logger.exception("mv_refresh_failed", extra={"view": mv.name})

            # Sleep for the smallest interval so we can wake up in time
            # for the next fast view refresh.
            await asyncio.sleep(min(fast_interval, slow_interval))
    except asyncio.CancelledError:
        logger.info("mv_refresh_cancelled")
        raise


async def _get_raw_connection():
    """Yield a raw AsyncConnection via the public engine accessor."""
    from soma_shared.db.session import get_engine

    async with get_engine().connect() as conn:
        yield conn
        await conn.commit()
