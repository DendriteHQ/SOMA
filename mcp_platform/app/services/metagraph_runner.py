from __future__ import annotations

import asyncio
import logging
import threading

from app.services.metagraph import MetagraphService
from app.core.logging import get_logger

logger = get_logger(__name__)


class MetagraphServiceRunner:
    def __init__(self, service: MetagraphService) -> None:
        self._service = service
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._started = threading.Event()

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._run, name="metagraph-service", daemon=True
        )
        self._thread.start()
        if not self._started.wait(timeout=0):
            logger.info("metagraph_thread_starting_async")

    def stop(self) -> None:
        loop = self._loop
        thread = self._thread
        if loop is None or thread is None:
            return
        future = asyncio.run_coroutine_threadsafe(self._service.stop(), loop)
        try:
            future.result(timeout=10)
        except Exception:
            logger.exception("metagraph_thread_stop_failed")
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=10)

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._started.set()
        try:
            loop.run_until_complete(self._service.start())
        except Exception:
            logger.exception("metagraph_thread_start_failed")
            return
        try:
            loop.run_forever()
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
