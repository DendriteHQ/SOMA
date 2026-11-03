"""Cached client for the internal metagraph service."""
from __future__ import annotations

import threading
from typing import Any

import httpx

from app.core.logging import get_logger


logger = get_logger(__name__)


class RemoteMetagraphService:
    """Expose the existing metagraph-service interface from a remote snapshot.

    Miner and validator routes need synchronous access to the latest snapshot.
    A small polling thread keeps that access local while the chain connection
    itself stays in the dedicated metagraph process.
    """

    def __init__(self, base_url: str, *, refresh_secs: float, timeout_secs: float) -> None:
        self._base_url = base_url.rstrip("/")
        self._refresh_secs = refresh_secs
        self._timeout_secs = timeout_secs
        self._latest_snapshot: dict[str, Any] | None = None
        self._connected = False
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def latest_snapshot(self) -> dict[str, Any] | None:
        with self._lock:
            return self._latest_snapshot

    @property
    def is_connected(self) -> bool:
        with self._lock:
            return self._connected

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="metagraph-client",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=self._timeout_secs + 1)
        self._thread = None

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                response = httpx.get(
                    f"{self._base_url}/api/private/metagraph/snapshot",
                    timeout=self._timeout_secs,
                )
                response.raise_for_status()
                snapshot = response.json()
                if not isinstance(snapshot, dict):
                    raise ValueError("metagraph snapshot response is not an object")
                with self._lock:
                    self._latest_snapshot = snapshot
                    self._connected = True
            except Exception as exc:
                with self._lock:
                    self._connected = False
                logger.warning("metagraph_client_refresh_failed", extra={"error": str(exc)})
            self._stop_event.wait(self._refresh_secs)

    def _economics(self) -> dict[str, Any]:
        response = httpx.get(
            f"{self._base_url}/api/private/metagraph/economics",
            timeout=self._timeout_secs,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("metagraph economics response is not an object")
        return payload

    def get_registration_cost_tao(self) -> float:
        return float(self._economics()["registration_cost_tao"])

    def get_alpha_price_tao(self) -> float:
        return float(self._economics()["alpha_price_tao"])
