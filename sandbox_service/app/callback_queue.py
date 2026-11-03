from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from soma_shared.contracts.sandbox.v1.messages import CompactBenchReportRequest


@dataclass(slots=True)
class CallbackQueueItem:
    id: int
    run_id: int
    report: CompactBenchReportRequest
    attempts: int
    next_attempt_at: float
    last_error: str | None


class CallbackQueue:
    """Durable local queue for callback payloads."""

    def __init__(self, db_path: Path):
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._conn:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS callback_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at REAL NOT NULL,
                    last_error TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_callback_queue_next_attempt_at "
                "ON callback_queue(next_attempt_at)"
            )
            # One row per dispatch whose report was queued, written in the same
            # transaction as the queue entry: a crash can neither lose a report nor
            # queue it twice. Keyed by dispatch, not run id, because the platform may
            # dispatch the same run to this host again.
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS reported_dispatches (
                    dispatch_id TEXT PRIMARY KEY,
                    run_id INTEGER NOT NULL,
                    created_at REAL NOT NULL
                )
                """
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def enqueue(self, report: CompactBenchReportRequest, *, dispatch_id: str | None = None) -> int | None:
        """Queue a report; None when this dispatch's report was queued before."""
        now = time.time()
        payload_json = json.dumps(report.model_dump(mode="json"), ensure_ascii=True)
        with self._lock:
            with self._conn:
                if dispatch_id is not None:
                    inserted = self._conn.execute(
                        "INSERT OR IGNORE INTO reported_dispatches (dispatch_id, run_id, created_at) "
                        "VALUES (?, ?, ?)",
                        (dispatch_id, report.run_id, now),
                    )
                    if inserted.rowcount == 0:
                        return None
                cursor = self._conn.execute(
                    """
                    INSERT INTO callback_queue (
                        run_id,
                        payload_json,
                        attempts,
                        next_attempt_at,
                        last_error,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, 0, ?, NULL, ?, ?)
                    """,
                    (report.run_id, payload_json, now, now, now),
                )
                return int(cursor.lastrowid)

    def was_reported(self, dispatch_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM reported_dispatches WHERE dispatch_id = ?", (dispatch_id,)
            ).fetchone()
        return row is not None

    def prune_reported(self, *, older_than_seconds: float) -> None:
        with self._lock:
            with self._conn:
                self._conn.execute(
                    "DELETE FROM reported_dispatches WHERE created_at < ?",
                    (time.time() - older_than_seconds,),
                )

    def fetch_due(self, *, now: float, limit: int) -> list[CallbackQueueItem]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, run_id, payload_json, attempts, next_attempt_at, last_error
                FROM callback_queue
                WHERE next_attempt_at <= ?
                ORDER BY next_attempt_at ASC, id ASC
                LIMIT ?
                """,
                (now, limit),
            ).fetchall()
        items: list[CallbackQueueItem] = []
        for row in rows:
            try:
                report = CompactBenchReportRequest.model_validate_json(row["payload_json"])
            except Exception:
                # If payload is corrupted, keep queue moving by dropping the entry.
                self.delete(int(row["id"]))
                continue
            items.append(
                CallbackQueueItem(
                    id=int(row["id"]),
                    run_id=int(row["run_id"]),
                    report=report,
                    attempts=int(row["attempts"]),
                    next_attempt_at=float(row["next_attempt_at"]),
                    last_error=str(row["last_error"]) if row["last_error"] else None,
                )
            )
        return items

    def delete(self, callback_id: int) -> None:
        with self._lock:
            with self._conn:
                self._conn.execute(
                    "DELETE FROM callback_queue WHERE id = ?",
                    (callback_id,),
                )

    def reschedule(
        self,
        *,
        callback_id: int,
        attempts: int,
        next_attempt_at: float,
        last_error: str,
    ) -> None:
        now = time.time()
        with self._lock:
            with self._conn:
                self._conn.execute(
                    """
                    UPDATE callback_queue
                    SET attempts = ?,
                        next_attempt_at = ?,
                        last_error = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (attempts, next_attempt_at, last_error, now, callback_id),
                )

    def stats(self) -> dict[str, int]:
        with self._lock:
            total = self._conn.execute("SELECT COUNT(*) FROM callback_queue").fetchone()[0]
            retry_backlog = self._conn.execute(
                "SELECT COUNT(*) FROM callback_queue WHERE attempts > 0"
            ).fetchone()[0]
            due_now = self._conn.execute(
                "SELECT COUNT(*) FROM callback_queue WHERE next_attempt_at <= ?",
                (time.time(),),
            ).fetchone()[0]
        return {
            "queued_callbacks": int(total),
            "callback_retry_backlog": int(retry_backlog),
            "callbacks_due_now": int(due_now),
        }
