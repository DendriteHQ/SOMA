"""Publish the current competition's task rows into the dataset validators read.

A SOMA task is two things: a pair of pre-built images, and a *row* describing it - the
problem statement the agent is given, the image references, the graded test command and
the FAIL_TO_PASS / PASS_TO_PASS ids the patch is judged on. The images are mirrored by
:mod:`dockerhub_task_sync`; this module does the same for the rows, into one Hugging
Face dataset repository that follows the same release schedule.

Until now those rows travelled as two files provisioned onto hosts by hand -
``tasks/soma_tasks.jsonl`` onto every sandbox host and ``tasks/soma_tasks_grading.jsonl``
onto every validator - which means a competition cannot start without an operator
touching every machine, and a validator with a stale file silently cannot grade part of
the competition.

``swe_bench_tasks`` records only *which* tasks a competition uses, never their content,
so the two halves come from different places:

* the **content** comes from ``HF_DATASET_SOURCE_FILE``, the same JSONL the importer was
  fed;
* the **selection** comes from the database.

Publishing their intersection is what makes the dataset exactly the current
competition's tasks. It is also the real guarantee that a future competition's tasks
are not in this repository: they are in the source file long before they may be
published, and only the database decides when they are allowed in. Nothing about the
repository's history or visibility protects them - see ``hf_registry.super_squash``.

The rules are the ones the image sync already follows:

* **Rows are added always, removed only while the repository is private.** Removing a
  row a validator is about to fetch breaks a grading it had no part in, and pruning is
  what empties the dataset at the start of a competition.
* **While the repository is public, only competitions whose own window is open may be
  added.** Competitions overlap at a handover, so "the tasks the database asks for" can
  include the *next* competition's hidden tasks while this one is still published.
* **A database or source-file failure aborts the tick.** An empty desired set is a
  legitimate instruction to empty the dataset, so it must not be reachable by accident.

Dispatch can be gated on the result once the sandbox and validator read the dataset
rather than a local file (``HF_DATASET_BLOCK_DISPATCH``): a run whose row is not
published cannot be graded, so holding it in ``pending`` beats failing it.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logging import get_logger
from app.services import benchmarks as benchmark_registry
from app.services import dockerhub_task_sync as image_sync
from app.services import hf_registry as hf

logger = get_logger(__name__)

BLOCK_REASON_SYNC_PENDING = "task_dataset_sync_pending"
BLOCK_REASON_ROW_MISSING = "task_dataset_row_missing"
BLOCK_REASON_DATASET_PRIVATE = "task_dataset_repository_private"

#: Repository root, so a relative HF_DATASET_SOURCE_FILE resolves the same way
#: wherever the process was started from.
_REPO_ROOT = Path(__file__).resolve().parents[3]

COMMIT_SUMMARY = "sync competition task rows"
SQUASH_SUMMARY = "competition task rows"


class TaskDatasetSyncError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# readiness snapshot
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TaskDatasetSnapshot:
    """What the dataset held at the end of the last sync tick."""

    repository: str
    ready_instance_ids: frozenset[str]
    pending_instance_ids: frozenset[str]
    updated_at: datetime


_SNAPSHOT: TaskDatasetSnapshot | None = None

# The visibility the last reconcile *observed*, for the same reason the image sync keeps
# it (see dockerhub_task_sync._PUBLIC): rows are committed while the repository is still
# private, so "the row is published" and "a sandbox can read it" are different claims,
# and only the second one makes a run dispatchable. ``None`` means not observed yet and
# does not block.
_PUBLIC: bool | None = None

# Whether the reconcile loop is actually running in this process; the gate is armed by
# the loop, not by the setting, for the reason spelled out in dockerhub_task_sync.
_LOOP_ARMED = False


def arm_dispatch_gate() -> None:
    global _LOOP_ARMED
    _LOOP_ARMED = True


def disarm_dispatch_gate() -> None:
    global _LOOP_ARMED
    _LOOP_ARMED = False


def dispatch_gate_armed() -> bool:
    return _LOOP_ARMED


def current_snapshot() -> TaskDatasetSnapshot | None:
    return _SNAPSHOT


def _publish_snapshot(snapshot: TaskDatasetSnapshot) -> None:
    global _SNAPSHOT
    _SNAPSHOT = snapshot


def reset_snapshot() -> None:
    """Drop the snapshot and the observed visibility (used by tests)."""
    global _SNAPSHOT, _PUBLIC
    _SNAPSHOT = None
    _PUBLIC = None


def publish_visibility(public: bool | None) -> None:
    """Record the visibility a reconcile observed for the dataset repository."""
    global _PUBLIC
    _PUBLIC = public


def observed_visibility() -> bool | None:
    return _PUBLIC


# ---------------------------------------------------------------------------
# configuration
# ---------------------------------------------------------------------------


def sync_enabled() -> bool:
    return bool(settings.hf_dataset_enabled)


def repository() -> str:
    return str(settings.hf_dataset_repository or "").strip()


def target_path() -> str:
    return str(settings.hf_dataset_target_path or "").strip() or "tasks.jsonl"


def source_file() -> Path:
    configured = str(settings.hf_dataset_source_file or "").strip()
    if not configured:
        raise TaskDatasetSyncError("HF_DATASET_SOURCE_FILE is required for dataset sync")
    path = Path(configured).expanduser()
    return path if path.is_absolute() else _REPO_ROOT / path


# ---------------------------------------------------------------------------
# rows
# ---------------------------------------------------------------------------


def parse_rows(payload: bytes | str) -> dict[str, dict]:
    """``instance_id`` -> row, from a JSONL payload.

    A row without an instance id is dropped: it cannot be selected by the database and
    cannot be looked up by a consumer, so it is not a task. Malformed lines are dropped
    with a warning rather than failing the whole tick - one bad line in the source file
    should not stop the other tasks of a competition from publishing.
    """
    text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
    rows: dict[str, dict] = {}
    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            logger.warning("hf_task_sync_malformed_row", extra={"line": line_number})
            continue
        if not isinstance(row, dict):
            continue
        instance_id = str(row.get("instance_id") or "").strip()
        if not instance_id:
            logger.warning("hf_task_sync_row_without_instance_id", extra={"line": line_number})
            continue
        rows[instance_id] = row
    return rows


def serialize_rows(rows: dict[str, dict]) -> bytes:
    """The published file's bytes, byte-identical for the same set of rows.

    Sorted by instance id with sorted keys inside each row, so "did anything change"
    is a byte comparison and an unchanged tick produces no commit. Without that the
    repository would grow a commit every tick.
    """
    return "".join(
        json.dumps(rows[instance_id], sort_keys=True, ensure_ascii=True) + "\n"
        for instance_id in sorted(rows)
    ).encode()


def load_source_rows() -> dict[str, dict]:
    """The task rows available to publish.

    A missing or unreadable source file raises: an empty row set is a legitimate
    instruction to empty the dataset, so it must never be the result of a mistake.
    """
    path = source_file()
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise TaskDatasetSyncError(
            f"Task dataset source file {path} could not be read: {exc}"
        ) from exc
    return parse_rows(payload)


async def load_desired_rows(
    db: AsyncSession,
    *,
    windows: list[tuple[int, datetime, datetime]],
    now: datetime,
    public: bool,
    source_rows: dict[str, dict],
) -> tuple[dict[str, dict], set[str]]:
    """``(rows to publish, instance ids the source file has no row for)``.

    Which competitions count, and which tasks they reference, is asked of
    ``dockerhub_task_sync`` rather than re-derived here. The two channels publish two
    halves of the same task, and a task the images sync considers part of a competition
    while the dataset sync does not (or the reverse) is a run that can never complete.
    """
    competition_ids = image_sync.relevant_competition_ids(windows, now=now)
    if public:
        competition_ids &= image_sync.publishable_competition_ids(windows, now=now)

    instance_ids = await image_sync.load_desired_instance_ids(
        db, competition_ids=competition_ids
    )

    desired = {
        instance_id: source_rows[instance_id]
        for instance_id in instance_ids
        if instance_id in source_rows
    }
    return desired, instance_ids - set(desired)


@dataclass(frozen=True)
class DatasetSyncPlan:
    rows: dict[str, dict]
    added: tuple[str, ...]
    updated: tuple[str, ...]
    removed: tuple[str, ...]
    unchanged: bool

    @property
    def is_empty(self) -> bool:
        return self.unchanged


def plan_dataset_sync(
    *,
    desired: dict[str, dict],
    published: dict[str, dict],
    prune: bool,
) -> DatasetSyncPlan:
    """Diff the published file against the database's selection.

    Without ``prune`` the published set never shrinks: the result is the union, with
    the desired row winning for an instance id in both, so a corrected row still lands
    while the repository is public but nothing disappears from under a validator.
    """
    rows = dict(desired) if prune else {**published, **desired}

    added = tuple(sorted(set(rows) - set(published)))
    removed = tuple(sorted(set(published) - set(rows)))
    updated = tuple(
        sorted(
            instance_id
            for instance_id in set(rows) & set(published)
            if rows[instance_id] != published[instance_id]
        )
    )
    return DatasetSyncPlan(
        rows=rows,
        added=added,
        updated=updated,
        removed=removed,
        unchanged=serialize_rows(rows) == serialize_rows(published),
    )


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------


def _apply_dataset_sync(
    *,
    repo: str,
    path: str,
    desired: dict[str, dict],
    prune: bool,
    squash: bool,
) -> dict:
    """Blocking Hugging Face work for one tick; run via ``asyncio.to_thread``."""
    if not hf.repository_exists(repo):
        raise TaskDatasetSyncError(
            f"Dataset repository {repo} does not exist or the token cannot see it. "
            "It is created by an operator, not by the platform: the competition token "
            "is scoped to an existing repository and a repository the platform created "
            "itself would be one nobody reviewed the visibility of."
        )

    published = parse_rows(hf.download_file(repo, path) or b"")
    plan = plan_dataset_sync(desired=desired, published=published, prune=prune)

    commit_sha = ""
    squashed = ""
    if not plan.unchanged:
        content = serialize_rows(plan.rows)
        hf.ensure_within_size_limit(path, content)
        commit_sha = hf.commit(
            repo,
            files={path: content},
            summary=f"{COMMIT_SUMMARY} ({len(plan.rows)} rows)",
        )
        if squash:
            # After every content commit, not only after a prune: the listing is then
            # always a single commit, which is the state a published repository has to
            # be in, and it stays that way across restarts without any state to track.
            # Best-effort - see run_dataset_sync_tick for why a failure here is logged
            # rather than raised.
            squashed = hf.super_squash(repo, summary=SQUASH_SUMMARY)

    return {
        "commit": commit_sha,
        "squashed": squashed,
        "added": plan.added,
        "updated": plan.updated,
        "removed": plan.removed,
        "unchanged": plan.unchanged,
        "published_instance_ids": set(plan.rows),
    }


def reconcile_visibility(*, public: bool) -> str:
    """Bring the dataset's visibility in line with the phase; blocking.

    Returns the same vocabulary as the image reconcile, so one log line can describe
    both channels: ``private``/``public`` for no change, ``changed_to_*`` for a flip,
    ``error: ...`` for a request that was accepted without moving the repository.
    """
    repo = repository()
    want_private = not public
    currently_private = hf.is_private(repo)
    publish_visibility(not currently_private)
    if currently_private == want_private:
        return "private" if currently_private else "public"

    if public:
        # A published repository whose listing is more than one commit deep walks back
        # through whatever it held before. That content was itself published in its own
        # window, so this is a hygiene problem rather than a leak of anything hidden -
        # and holding the flip back would stall grading for the whole competition,
        # which is strictly worse. So: publish, and say so.
        depth = hf.commit_count(repo)
        if depth > 1:
            logger.warning(
                "hf_dataset_history_not_squashed",
                extra={"repository": repo, "commits": depth},
            )

    now_private = hf.set_private(repo, private=want_private)
    publish_visibility(not now_private)
    if now_private != want_private:
        return (
            "error: visibility unchanged after request "
            f"(wanted {'private' if want_private else 'public'}, "
            f"still {'private' if now_private else 'public'})"
        )
    return f"changed_to_{'private' if want_private else 'public'}"


async def run_dataset_sync_tick(
    *,
    db: AsyncSession,
    windows: list[tuple[int, datetime, datetime]],
    public: bool,
    now: datetime | None = None,
) -> dict:
    """Bring the published dataset in line with ``swe_bench_tasks``.

    ``public`` is the visibility the caller is about to enforce for this same tick;
    removals - and the set of competitions allowed to be added - both depend on it.
    """
    now = now or datetime.now(timezone.utc)
    repo = repository()
    if not repo:
        raise TaskDatasetSyncError("HF_DATASET_REPOSITORY is required for dataset sync")
    path = target_path()

    source_rows = load_source_rows()
    desired, missing_from_source = await load_desired_rows(
        db, windows=windows, now=now, public=public, source_rows=source_rows
    )

    prune = bool(settings.hf_dataset_prune) and not public
    result = await asyncio.to_thread(
        _apply_dataset_sync,
        repo=repo,
        path=path,
        desired=desired,
        prune=prune,
        squash=bool(settings.hf_dataset_squash_on_prune),
    )

    published_instance_ids: set[str] = result.pop("published_instance_ids")
    ready = set(desired) & published_instance_ids
    _publish_snapshot(
        TaskDatasetSnapshot(
            repository=repo,
            ready_instance_ids=frozenset(ready),
            pending_instance_ids=frozenset(set(desired) | missing_from_source) - ready,
            updated_at=now,
        )
    )

    summary = {
        **result,
        "repository": repo,
        "path": path,
        "desired_tasks": len(desired),
        "ready_tasks": len(ready),
        "missing_from_source": sorted(missing_from_source),
        "pruned": prune,
    }
    if missing_from_source:
        # The database asks for a task the source file has no row for. Nothing can
        # publish it, so it is the operator's cue that the file is behind the import.
        logger.warning("hf_task_sync_rows_missing_from_source", extra=summary)
    if result["unchanged"]:
        logger.debug("hf_task_sync_unchanged", extra=summary)
    else:
        logger.info("hf_task_sync_reconciled", extra=summary)
    return summary


# ---------------------------------------------------------------------------
# dispatch gate
# ---------------------------------------------------------------------------


def dispatch_block_reason(
    *,
    benchmark_name: str | None,
    instance_id: str | None,
    screener_stage: int | None = None,
) -> str | None:
    """Why this task must not be dispatched yet, or ``None`` if it may be.

    Off unless ``HF_DATASET_BLOCK_DISPATCH`` is set, because it is only true that a
    missing row stops a run once the sandbox and the validator actually read the
    dataset; while they read a file provisioned onto the host, an unpublished row
    stops nothing and gating on it would hold back runs that would have succeeded.
    """
    if not _LOOP_ARMED:
        return None
    if not sync_enabled() or not bool(settings.hf_dataset_block_dispatch):
        return None
    # Resolved by the same helper the two syncs use, so a row nobody publishes cannot
    # be a row everybody waits for.
    if not benchmark_registry.is_soma_task(benchmark_name, screener_stage=screener_stage):
        return None

    instance = str(instance_id or "").strip()
    if not instance:
        return None

    snapshot = _SNAPSHOT
    if snapshot is None:
        return BLOCK_REASON_SYNC_PENDING
    # Checked before readiness: a private dataset stops every task, so reporting the
    # repository is more useful than reporting one task's row.
    if _PUBLIC is False:
        return BLOCK_REASON_DATASET_PRIVATE
    if instance in snapshot.ready_instance_ids:
        return None
    return BLOCK_REASON_ROW_MISSING
