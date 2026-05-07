from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json
import logging
import time

import numpy as np


@dataclass
class WeightSnapshot:
    recorded_at_unix: float
    uids: np.ndarray
    weights: np.ndarray

    def to_dict(self) -> dict[str, Any]:
        return {
            "recorded_at_unix": float(self.recorded_at_unix),
            "uids": self.uids.astype(int).tolist(),
            "weights": self.weights.astype(float).tolist(),
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "WeightSnapshot | None":
        try:
            timestamp = float(raw.get("recorded_at_unix"))
            raw_uids = raw.get("uids")
            raw_weights = raw.get("weights")
            if not isinstance(raw_uids, list) or not isinstance(raw_weights, list):
                return None
            if len(raw_uids) == 0 or len(raw_uids) != len(raw_weights):
                return None
            uids = np.array([int(uid) for uid in raw_uids], dtype=np.int64)
            weights = np.array([float(weight) for weight in raw_weights], dtype=np.float32)
            return cls(recorded_at_unix=timestamp, uids=uids, weights=weights)
        except Exception:
            return None


class WeightHistoryStore:
    def __init__(self, file_path: str, max_entries: int):
        self.file_path = Path(file_path)
        self.max_entries = max(1, int(max_entries))

    def append(
        self,
        uids: np.ndarray,
        weights: np.ndarray,
        *,
        recorded_at_unix: float | None = None,
    ) -> None:
        if len(uids) == 0 or len(weights) == 0 or len(uids) != len(weights):
            logging.warning(
                "Skipping weight history append: invalid uids/weights shape (uids=%s, weights=%s)",
                len(uids),
                len(weights),
            )
            return

        snapshot = WeightSnapshot(
            recorded_at_unix=recorded_at_unix if recorded_at_unix is not None else time.time(),
            uids=np.array(uids, dtype=np.int64),
            weights=np.array(weights, dtype=np.float32),
        )
        entries = self._load_raw_entries()
        entries.append(snapshot.to_dict())
        if len(entries) > self.max_entries:
            entries = entries[-self.max_entries :]
        self._write_raw_entries(entries)

    def latest(self) -> WeightSnapshot | None:
        entries = self._load_raw_entries()
        for raw in reversed(entries):
            if not isinstance(raw, dict):
                continue
            snapshot = WeightSnapshot.from_dict(raw)
            if snapshot is not None:
                return snapshot
        return None

    def _load_raw_entries(self) -> list[dict[str, Any]]:
        if not self.file_path.exists():
            return []
        try:
            raw = json.loads(self.file_path.read_text(encoding="utf-8"))
            if isinstance(raw, list):
                # Backward compatibility for possible legacy plain-list format.
                return raw
            if isinstance(raw, dict):
                entries = raw.get("entries", [])
                if isinstance(entries, list):
                    return entries
            logging.warning(
                "Unexpected weight history format in %s, ignoring file",
                self.file_path,
            )
            return []
        except Exception as exc:
            logging.warning(
                "Failed to read weight history from %s: %s",
                self.file_path,
                exc,
            )
            return []

    def _write_raw_entries(self, entries: list[dict[str, Any]]) -> None:
        try:
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {"version": 1, "entries": entries}
            tmp_path = self.file_path.with_suffix(f"{self.file_path.suffix}.tmp")
            tmp_path.write_text(
                json.dumps(payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            tmp_path.replace(self.file_path)
        except Exception as exc:
            logging.warning(
                "Failed to persist weight history to %s: %s",
                self.file_path,
                exc,
            )
