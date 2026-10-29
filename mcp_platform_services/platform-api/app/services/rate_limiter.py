from __future__ import annotations

import time
from collections import deque
from typing import Callable


class SlidingWindowRateLimiter:
    """Allow at most ``max_requests`` per ``window_seconds`` for each key (e.g. a miner hotkey).

    Like the nonce cache, the state is process-local: it resets on restart and is not
    shared between replicas. Callers must key on an authenticated identity, otherwise
    anyone can exhaust someone else's budget.
    """

    def __init__(
        self,
        *,
        max_requests: int,
        window_seconds: float,
        max_keys: int = 10_000,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._max_keys = max_keys
        self._clock = clock
        self._hits: dict[str, deque[float]] = {}

    @property
    def enabled(self) -> bool:
        return self._max_requests > 0 and self._window_seconds > 0

    def hit(self, key: str) -> float | None:
        """Record a request for ``key``; return seconds to wait when over the limit, else None.

        Rejected requests are not recorded, so a client retrying too early does not
        push its own window further out.
        """
        if not self.enabled:
            return None
        now = self._clock()
        window_start = now - self._window_seconds
        hits = self._hits.get(key)
        if hits is None:
            # Any keypair can sign, so the number of keys is attacker-controlled.
            if len(self._hits) >= self._max_keys:
                self._prune(window_start)
            hits = self._hits[key] = deque()
        while hits and hits[0] <= window_start:
            hits.popleft()
        if len(hits) >= self._max_requests:
            return max(0.0, hits[0] - window_start)
        hits.append(now)
        return None

    def _prune(self, window_start: float) -> None:
        stale = [key for key, hits in self._hits.items() if not hits or hits[-1] <= window_start]
        for key in stale:
            del self._hits[key]
        if len(self._hits) >= self._max_keys:
            # Still full of active keys: drop the least recently used ones.
            by_last_hit = sorted(self._hits, key=lambda key: self._hits[key][-1])
            for key in by_last_hit[: len(self._hits) - self._max_keys + 1]:
                del self._hits[key]
