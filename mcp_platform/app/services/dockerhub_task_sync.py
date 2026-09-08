"""Mirror the current competition's task images into the repository validators pull.

SOMA task images are built into a long-lived source repository
(``DOCKERHUB_TASK_SOURCE_REPOSITORY``) that accumulates every task ever built. That
repository can never be published: it holds the tasks of future competitions too.

So the competition repository (``DOCKERHUB_TASK_TARGET_REPOSITORY``) is derived from
``swe_bench_tasks`` instead - it contains exactly the images of the SOMA tasks the
current competitions actually reference, and nothing else. Publishing it during the
evaluation window (``dockerhub_visibility``) then exposes precisely those tasks.

The sync is a reconcile, not a reaction to events: each tick computes the tag set the
database implies and moves the repository towards it. Nothing has to be persisted, a
missed tick or a restart self-corrects, and importing tasks needs no separate step -
they appear in the repository on the next tick.

Two rules keep the destructive half safe:

* **Copies always, deletions only while the repository is private.** Pruning is what
  empties the repository at the start of a competition (its task set is still empty, or
  holds only the new tasks), and it is exactly the operation that must never run while
  a validator is pulling. The visibility state the same tick is about to enforce
  decides whether pruning is allowed.
* **While the repository is public, only competitions whose own window is open may be
  copied in.** Competitions overlap at a handover, so the tasks the database asks for
  can include the *next* competition's hidden tasks while this one is still published.
* **A database read failure aborts the whole tick.** An empty desired set is a
  legitimate instruction to empty the repository, so it must not be reachable by
  accident.

Dispatch is gated on the result. A run whose task images are not in the competition
repository yet cannot be graded and its sandbox cannot even pull the env image, so
:func:`dispatch_block_reason` holds those runs back in ``pending`` until the copy has
landed rather than letting them fail for a reason the miner had no part in.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logging import get_logger
from app.services import benchmarks as benchmark_registry
from app.services import dockerhub_registry as hub
from soma_shared.db.models.swe_bench_task import SweBenchTask

logger = get_logger(__name__)

#: The grading tag. ``<instance_id>`` is the dind env image the sandbox runs the agent
#: in; ``<instance_id>.test`` is the image the validator grades the patch on. Both are
#: required for a task to be runnable, so readiness is all-or-nothing per task.
TEST_TAG_SUFFIX = ".test"

TARGET_REPOSITORY_DESCRIPTION = (
    "SOMA hidden competition tasks (screener-2 + evaluation): dind env images and "
    "grading test images. Managed by the platform - contents are derived from "
    "swe_bench_tasks."
)

BLOCK_REASON_SYNC_PENDING = "task_images_sync_pending"
BLOCK_REASON_IMAGES_MISSING = "task_images_missing"


class TaskSyncError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# readiness snapshot
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TaskImageSnapshot:
    """What the competition repository held at the end of the last sync tick.

    Read by the orchestrator on every dispatch decision, so it is a frozen value
    published as a whole rather than a mutable structure being updated in place.
    """

    target_repository: str
    ready_instance_ids: frozenset[str]
    pending_instance_ids: frozenset[str]
    updated_at: datetime


_SNAPSHOT: TaskImageSnapshot | None = None

# Whether the reconcile loop is actually running in this process. The gate below is
# armed by the loop, not by the setting: the loop is started from the visibility task,
# which a deployment can have switched off (or which can fail to start - main.py
# treats that as non-fatal). An armed gate with nothing to feed it would hold every
# SOMA run in `pending` forever, so an unfed gate stays open instead.
_LOOP_ARMED = False


def arm_dispatch_gate() -> None:
    global _LOOP_ARMED
    _LOOP_ARMED = True


def disarm_dispatch_gate() -> None:
    global _LOOP_ARMED
    _LOOP_ARMED = False


def dispatch_gate_armed() -> bool:
    return _LOOP_ARMED


def current_snapshot() -> TaskImageSnapshot | None:
    return _SNAPSHOT


def _publish_snapshot(snapshot: TaskImageSnapshot) -> None:
    global _SNAPSHOT
    _SNAPSHOT = snapshot


def reset_snapshot() -> None:
    """Drop the snapshot, so readiness is unknown again (used by tests)."""
    global _SNAPSHOT
    _SNAPSHOT = None


# ---------------------------------------------------------------------------
# configuration
# ---------------------------------------------------------------------------


def sync_enabled() -> bool:
    return bool(settings.dockerhub_task_sync_enabled)


def target_repository() -> str:
    """The repository the platform mirrors into and publishes.

    Falls back to the visibility list, because in the single-repository setup that
    list already names it and repeating it in a second variable is one more thing to
    get out of agreement.
    """
    explicit = str(settings.dockerhub_task_target_repository or "").strip()
    if explicit:
        return explicit
    for repository in settings.dockerhub_task_repositories:
        candidate = str(repository or "").strip()
        if candidate:
            return candidate
    return ""


def source_repository() -> str:
    return str(settings.dockerhub_task_source_repository or "").strip()


# ---------------------------------------------------------------------------
# desired state
# ---------------------------------------------------------------------------


def task_tags(instance_id: str) -> tuple[str, str]:
    instance = str(instance_id or "").strip()
    return instance, f"{instance}{TEST_TAG_SUFFIX}"


def relevant_competition_ids(
    windows: list[tuple[int, datetime, datetime]],
    *,
    now: datetime,
) -> set[int]:
    """Competitions whose images may still be needed.

    A competition is included from the moment it is configured until its public window
    has closed - the images have to be in place *before* the window opens, and cannot
    be removed while it is open. Competitions overlap at a handover, so this is a set
    rather than "the current competition".
    """
    return {
        competition_id
        for competition_id, _start, end in windows
        if now < end
    }


def publishable_competition_ids(
    windows: list[tuple[int, datetime, datetime]],
    *,
    now: datetime,
) -> set[int]:
    """Competitions whose own public window is open.

    Narrower than :func:`relevant_competition_ids`, which also covers a competition
    being prepared. The difference matters at a handover: competitions overlap, so
    "the tasks the database asks for" can include the *next* competition's hidden
    tasks while this one is still published. Those may be staged into the repository
    while it is private, but adding them while it is public would publish them.

    Shared with the dataset sync, so both halves of a task - its images and its rows -
    are held back by the same rule.
    """
    return {
        competition_id
        for competition_id, start, end in windows
        if start <= now < end
    }


async def load_desired_instance_ids(
    db: AsyncSession,
    *,
    competition_ids: set[int],
) -> set[str]:
    """The SOMA task instance ids the given competitions reference.

    SWE-bench tasks are filtered out: their instances come from a public Hugging Face
    dataset and their environment images follow SWE-bench's own naming, so nothing
    about them lives in this repository.
    """
    if not competition_ids:
        return set()

    rows = (
        await db.execute(
            select(
                SweBenchTask.instance_id,
                SweBenchTask.benchmark_name,
                SweBenchTask.screener_stage,
            ).where(SweBenchTask.competition_fk.in_(competition_ids))
        )
    ).all()

    return {
        str(instance_id).strip()
        for instance_id, benchmark_name, screener_stage in rows
        if str(instance_id or "").strip()
        and benchmark_registry.is_soma_task(
            benchmark_name, screener_stage=screener_stage
        )
    }


@dataclass(frozen=True)
class TagSyncPlan:
    to_copy: tuple[str, ...]
    to_delete: tuple[str, ...]

    @property
    def is_empty(self) -> bool:
        return not self.to_copy and not self.to_delete


def plan_tag_sync(
    *,
    desired_instance_ids: set[str],
    present_tags: set[str],
    prune: bool,
) -> TagSyncPlan:
    """Diff the repository against the database.

    Pruning removes *every* tag the database does not ask for, including tags nobody
    recognises - a repository that keeps leftovers is not the clean, exactly-this-
    competition set the published window is supposed to expose.
    """
    desired_tags: set[str] = set()
    for instance_id in desired_instance_ids:
        desired_tags.update(task_tags(instance_id))

    to_copy = sorted(desired_tags - present_tags)
    to_delete = sorted(present_tags - desired_tags) if prune else []
    return TagSyncPlan(to_copy=tuple(to_copy), to_delete=tuple(to_delete))


def ready_instance_ids(
    *,
    desired_instance_ids: set[str],
    present_tags: set[str],
) -> set[str]:
    """Tasks whose env *and* test image are both in the repository."""
    return {
        instance_id
        for instance_id in desired_instance_ids
        if all(tag in present_tags for tag in task_tags(instance_id))
    }


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------


def _apply_tag_sync(
    *,
    source: str,
    target: str,
    desired_instance_ids: set[str],
    prune: bool,
) -> dict:
    """Blocking Docker Hub work for one tick; run via ``asyncio.to_thread``."""
    jwt = hub.login()
    created = hub.create_private_repository(
        target, jwt=jwt, description=TARGET_REPOSITORY_DESCRIPTION
    )
    present_tags = set() if created else hub.list_tag_names(target, jwt=jwt)

    plan = plan_tag_sync(
        desired_instance_ids=desired_instance_ids,
        present_tags=present_tags,
        prune=prune,
    )

    copied: dict[str, str] = {}
    errors: dict[str, str] = {}
    if plan.to_copy:
        token = hub.copy_scope_token(source=source, target=target)
        for tag in plan.to_copy:
            try:
                outcome = hub.copy_tag(source=source, target=target, tag=tag, token=token)
            except hub.DockerHubError as exc:
                # One unbuilt or unreachable task must not stop the others from
                # landing: the tasks that do copy stay dispatchable.
                errors[tag] = str(exc)
                continue
            if outcome == "missing-in-source":
                errors[tag] = f"missing in {source}"
                continue
            copied[tag] = outcome
            present_tags.add(tag)

    deleted: list[str] = []
    for tag in plan.to_delete:
        try:
            hub.delete_tag(target, tag, jwt=jwt)
        except hub.DockerHubError as exc:
            errors[tag] = str(exc)
            continue
        deleted.append(tag)
        present_tags.discard(tag)

    return {
        "created_repository": created,
        "copied": copied,
        "deleted": deleted,
        "errors": errors,
        "present_tags": present_tags,
    }


async def run_task_sync_tick(
    *,
    db: AsyncSession,
    windows: list[tuple[int, datetime, datetime]],
    public: bool,
    now: datetime | None = None,
) -> dict:
    """Bring the competition repository in line with ``swe_bench_tasks``.

    ``public`` is the visibility the caller is about to enforce for this same tick;
    deletions are only allowed when that is private.
    """
    now = now or datetime.now(timezone.utc)
    target = target_repository()
    source = source_repository()
    if not target or not source:
        raise TaskSyncError(
            "DOCKERHUB_TASK_SOURCE_REPOSITORY and a target repository are required"
        )

    competition_ids = relevant_competition_ids(windows, now=now)
    if public:
        # Copies are unconditional, so the set they are computed from is what keeps a
        # competition being prepared out of a published repository (see
        # publishable_competition_ids). Its images are staged on the next private tick.
        competition_ids &= publishable_competition_ids(windows, now=now)
    desired_instance_ids = await load_desired_instance_ids(
        db, competition_ids=competition_ids
    )

    prune = bool(settings.dockerhub_task_sync_prune) and not public
    result = await asyncio.to_thread(
        _apply_tag_sync,
        source=source,
        target=target,
        desired_instance_ids=desired_instance_ids,
        prune=prune,
    )

    present_tags: set[str] = result.pop("present_tags")
    ready = ready_instance_ids(
        desired_instance_ids=desired_instance_ids, present_tags=present_tags
    )
    _publish_snapshot(
        TaskImageSnapshot(
            target_repository=target,
            ready_instance_ids=frozenset(ready),
            pending_instance_ids=frozenset(desired_instance_ids - ready),
            updated_at=now,
        )
    )

    summary = {
        **result,
        "target": target,
        "source": source,
        "competitions": sorted(competition_ids),
        "desired_tasks": len(desired_instance_ids),
        "ready_tasks": len(ready),
        "pruned": prune,
    }
    if result["copied"] or result["deleted"] or result["errors"]:
        logger.info("dockerhub_task_sync_reconciled", extra=summary)
    else:
        logger.debug("dockerhub_task_sync_unchanged", extra=summary)
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

    Only SOMA tasks are gated, and only while the reconcile loop is running in this
    process. Before that loop's first tick has completed the answer is "not yet"
    rather than "go ahead": the repository may still hold the previous competition's
    tags, and a run dispatched against a task whose images are not there fails in the
    sandbox.
    """
    if not _LOOP_ARMED:
        return None
    if not sync_enabled() or not bool(settings.dockerhub_task_sync_block_dispatch):
        return None
    # Resolved here rather than by the caller, so this and the sync's own filter
    # cannot answer differently for the same row (see benchmarks.is_soma_task).
    if not benchmark_registry.is_soma_task(benchmark_name, screener_stage=screener_stage):
        return None

    instance = str(instance_id or "").strip()
    if not instance:
        return None

    snapshot = _SNAPSHOT
    if snapshot is None:
        return BLOCK_REASON_SYNC_PENDING
    if instance in snapshot.ready_instance_ids:
        return None
    return BLOCK_REASON_IMAGES_MISSING
