"""Open the hidden-task image repositories for the window validators need them.

SOMA task lists are graded from container images: the validator pulls a task's test
image, applies the miner's patch inside it and runs the task's own graded test command
(see ``validator/evaluation/soma_task_evaluator.py``). Those images are built into a
private Docker Hub repository, because publishing a competition's hidden tasks ahead of
time would let miners look at the tests they are about to be scored on.

Validators are independent operators, so handing each of them registry credentials for
that private repository means distributing, rotating and revoking a shared secret across
machines the subnet does not control. Instead the platform - which already holds the
Docker Hub token - flips the repository's visibility on the competition's own schedule:
public for the evaluation window, private again once it has closed. A public repository
needs no credentials at all on the validator side.

The window is derived from ``competition_timeframes``. Both phases that use hidden
tasks - screener stage 2 and full evaluation - run inside the *evaluation* window:
stage-2 seeding is gated on ``now >= eval_starts_at`` (see
``swebench_orchestrator._seed_runs_for_competition``), and full evaluation follows once
the stage-2 cohort has been ranked. The stretch between ``upload_ends_at`` and
``eval_starts_at`` is idle - no hidden-task run exists yet.

        upload_starts_at          upload_ends_at            eval_starts_at                  eval_ends_at
         |                         |                         |                               |
         |--- stage 1, uploads ----|--------- idle ----------|-- stage 2, then evaluation ---|
         |                         |                         |                               |
                                                             |----- task images public ------|

So ``eval_starts_at`` (the default) already covers every hidden-task run: the images go
public exactly when the first stage-2 run can be dispatched, not after stage 2.

``DOCKERHUB_VISIBILITY_PUBLIC_FROM=upload_ends_at`` moves the opening back into the idle
stretch. That grades nothing extra - there is nothing to grade there yet - and it only
lengthens the exposure window, so it is not the default.

The boundary between "dispatch may begin" and "the repository is open" is handled in two
places instead. ``DOCKERHUB_VISIBILITY_INTERVAL_SECONDS`` is a ceiling rather than a
cadence: each tick records the next window edge and the loop wakes just after it, so the
flip lands on ``eval_starts_at`` instead of up to one interval later. And because a tick
can still be late or fail, both dispatch gates are told the visibility this tick
*observed* - a run is only released once the repository it must pull from is actually
public, so the worst case is a run waiting rather than a run failing.

It closes at ``eval_ends_at`` exactly. Nothing is meant to be in flight there: a
competition's timeframes are set with enough slack that every run has been dispatched,
executed and graded before the evaluation window ends, so the moment the window closes
there is nothing left that needs to pull. Closing on the boundary is what lets the next
competition be staged straight away - the repository can only be emptied while it is
private (see ``dockerhub_task_sync``), so every minute the window stays open past the
end is a minute the next competition cannot be prepared in.

This reconciles rather than reacting to events: every tick computes the visibility the
current time implies and only calls Docker Hub when it disagrees. A missed tick, a
restart, or a manual change in the Docker Hub UI therefore self-corrects, and no state
has to be persisted anywhere.

The same tick also reconciles the repository's *contents* (``dockerhub_task_sync``), so
that what gets published is exactly the current competition's tasks. Ordering within a
tick is deliberate: contents are brought up to date first, and the sync only deletes
tags when this tick has determined the repository stays private.

Despite the module's name this is the release schedule for *two* channels, because a
task is not runnable without both of them: the images (Docker Hub) and the rows that
describe them - problem statement, image references, graded test ids - which are
published to a Hugging Face dataset by ``hf_task_sync``. They deliberately share this
one loop rather than running two: the window is computed once, the ordering
(contents, then visibility) is the same for both, and a deployment where the images and
the rows describing them open at different moments has no useful meaning. The settings
keep their ``DOCKERHUB_VISIBILITY_*`` names for the same reason - they name the
schedule, not the registry.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logging import get_logger
from app.services import dockerhub_registry as hub
from app.services import dockerhub_task_sync
from app.services import hf_task_sync
from soma_shared.db.models.competition_config import CompetitionConfig
from soma_shared.db.models.competition_timeframe import CompetitionTimeframe
from soma_shared.db.session import get_db_session

logger = get_logger(__name__)

PUBLIC_FROM_EVAL_START = "eval_starts_at"
PUBLIC_FROM_UPLOAD_END = "upload_ends_at"

# The next window boundary the loop should wake up on, published by each tick. Without
# it the loop only reconciles every DOCKERHUB_VISIBILITY_INTERVAL_SECONDS, so a
# repository stays private for up to one interval past eval_starts_at - and dispatch
# begins at eval_starts_at exactly. The dispatch gates hold runs back through that gap
# rather than failing them, but there is no reason to make them wait it out: the
# boundaries are known in advance, so the flip can land on them.
_NEXT_BOUNDARY: datetime | None = None

# Never sleep less than this, so a boundary that has just passed (or a clock that moves
# backwards) cannot turn the loop into a busy wait against the Docker Hub API.
_MIN_SLEEP_SECONDS = 5.0

# The Docker Hub client lives in dockerhub_registry, which the operator CLI shares.
# Kept as module-level aliases so the calls below (and the tests) have one name to
# reach for.
DockerHubVisibilityError = hub.DockerHubError
_login = hub.login
_split_repository = hub.split_repository
_is_private = hub.is_private
_set_private = hub.set_private


# ---------------------------------------------------------------------------
# window computation
# ---------------------------------------------------------------------------


def _as_utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def public_window_for_timeframe(
    timeframe: CompetitionTimeframe,
    *,
    public_from: str,
) -> tuple[datetime, datetime]:
    """The [start, end) interval in which a competition's task images are public.

    ``eval_starts_at`` is when the first hidden-task run can be dispatched, so it is
    also when the images are first needed; ``upload_ends_at`` only opens earlier, in
    the idle stretch before that (see the module docstring).
    """
    if public_from == PUBLIC_FROM_UPLOAD_END:
        start = _as_utc(timeframe.upload_ends_at)
    else:
        start = _as_utc(timeframe.eval_starts_at)
    return start, _as_utc(timeframe.eval_ends_at)


async def load_active_public_windows(db: AsyncSession) -> list[tuple[int, datetime, datetime]]:
    """``(competition_id, public_from, public_until)`` for every active competition.

    Reads all active competitions rather than only the current one: competitions
    overlap around a handover (the previous one's evaluation window can still be open
    while the next one's upload window has started), and the repository must stay
    public while any of them still needs it.
    """
    rows = (
        await db.execute(
            select(
                CompetitionConfig.competition_fk,
                CompetitionTimeframe.upload_ends_at,
                CompetitionTimeframe.eval_starts_at,
                CompetitionTimeframe.eval_ends_at,
            )
            .join(
                CompetitionTimeframe,
                CompetitionTimeframe.competition_config_fk == CompetitionConfig.id,
            )
            .where(CompetitionConfig.is_active.is_(True))
        )
    ).all()

    public_from = str(settings.dockerhub_visibility_public_from)

    windows: list[tuple[int, datetime, datetime]] = []
    for row in rows:
        timeframe = CompetitionTimeframe(
            upload_ends_at=row.upload_ends_at,
            eval_starts_at=row.eval_starts_at,
            eval_ends_at=row.eval_ends_at,
        )
        start, end = public_window_for_timeframe(
            timeframe,
            public_from=public_from,
        )
        windows.append((int(row.competition_fk), start, end))
    return windows


def should_be_public(
    windows: list[tuple[int, datetime, datetime]],
    *,
    now: datetime,
) -> tuple[bool, int | None]:
    """Whether any active competition's public window contains ``now``.

    Returns the competition id that opened it, for the log line - with no active
    window the answer is private, which is also the safe default when the platform
    has no competition configured at all.
    """
    for competition_id, start, end in windows:
        if start <= now < end:
            return True, competition_id
    return False, None


# ---------------------------------------------------------------------------
# reconcile
# ---------------------------------------------------------------------------


def _reconcile_repositories(repositories: list[str], *, public: bool) -> dict[str, str]:
    """Blocking Docker Hub work for one tick; run via asyncio.to_thread."""
    jwt = _login()
    outcomes: dict[str, str] = {}
    want_private = not public
    for repository in repositories:
        try:
            currently_private = _is_private(repository, jwt=jwt)
            if currently_private == want_private:
                outcomes[repository] = "private" if currently_private else "public"
                continue
            now_private = _set_private(repository, jwt=jwt, private=want_private)
            if now_private != want_private:
                # The API accepted the request but the repository did not move, so a
                # silently ignored request cannot be reported as a successful flip.
                outcomes[repository] = (
                    "error: visibility unchanged after request "
                    f"(wanted {'private' if want_private else 'public'}, "
                    f"still {'private' if now_private else 'public'})"
                )
                continue
            outcomes[repository] = (
                f"changed_to_{'private' if want_private else 'public'}"
            )
        except DockerHubVisibilityError as exc:
            outcomes[repository] = f"error: {exc}"
    return outcomes


def next_window_boundary(
    windows: list[tuple[int, datetime, datetime]],
    *,
    now: datetime,
) -> datetime | None:
    """The earliest window edge still ahead of ``now``, opening or closing."""
    edges = [edge for _cid, start, end in windows for edge in (start, end) if edge > now]
    return min(edges) if edges else None


def _observed_visibility(outcomes: dict[str, str], repository: str) -> bool | None:
    """Whether ``repository`` ended this tick public, or ``None`` if unknown.

    ``_reconcile_repositories`` reports what it saw rather than what it wanted, so a
    request the API accepted without applying reads back as an error here - and an
    error is "unknown", never "public".
    """
    outcome = outcomes.get(repository)
    if outcome in {"public", "changed_to_public"}:
        return True
    if outcome in {"private", "changed_to_private"}:
        return False
    return None


# ---------------------------------------------------------------------------
# reconcile loop
# ---------------------------------------------------------------------------


async def run_visibility_tick(now: datetime | None = None) -> dict[str, str]:
    """Bring every configured repository in line with the current competition phase.

    Contents first, visibility second, in one tick and in that order: the images have
    to be in the repository before it is published, and the sync's destructive half is
    only allowed to run when this tick is about to leave the repository private (see
    ``dockerhub_task_sync``).
    """
    repositories = [
        repository.strip()
        for repository in settings.dockerhub_task_repositories
        if repository.strip()
    ]
    if not repositories and not hf_task_sync.sync_enabled():
        return {}

    now = now or datetime.now(timezone.utc)
    windows: list[tuple[int, datetime, datetime]] = []
    public = False
    competition_id: int | None = None
    async for db in get_db_session():
        windows = await load_active_public_windows(db)
        public, competition_id = should_be_public(windows, now=now)
        global _NEXT_BOUNDARY
        _NEXT_BOUNDARY = next_window_boundary(windows, now=now)
        if dockerhub_task_sync.sync_enabled():
            try:
                await dockerhub_task_sync.run_task_sync_tick(
                    db=db, windows=windows, public=public, now=now
                )
            except Exception:
                # A content sync that fails must not hold back the visibility flip:
                # validators still need whatever images did make it in, and staying
                # private would stall grading for the whole competition. Runs whose
                # images are missing are held back at dispatch instead.
                logger.exception("dockerhub_task_sync_failed")
        if hf_task_sync.sync_enabled():
            try:
                await hf_task_sync.run_dataset_sync_tick(
                    db=db, windows=windows, public=public, now=now
                )
            except Exception:
                # Same reasoning as above, and independent of it: one channel failing
                # must not take the other one down with it.
                logger.exception("hf_task_sync_failed")
        break

    outcomes: dict[str, str] = {}
    if repositories:
        outcomes = await asyncio.to_thread(
            _reconcile_repositories, repositories, public=public
        )
    # What the reconcile *observed* for the repository dispatch depends on, so a run is
    # only released once the repository it has to pull from is actually public. An
    # outcome we cannot read (an error, or a target this deployment does not manage)
    # reports None, which does not gate dispatch - see dockerhub_task_sync._PUBLIC.
    dockerhub_task_sync.publish_visibility(
        _observed_visibility(outcomes, dockerhub_task_sync.target_repository())
    )
    if hf_task_sync.sync_enabled():
        try:
            outcomes[f"datasets/{hf_task_sync.repository()}"] = await asyncio.to_thread(
                hf_task_sync.reconcile_visibility, public=public
            )
        except Exception as exc:
            logger.exception("hf_dataset_visibility_failed")
            outcomes[f"datasets/{hf_task_sync.repository()}"] = f"error: {exc}"

    changed = {
        repository: outcome
        for repository, outcome in outcomes.items()
        if outcome.startswith("changed_to_")
    }
    errors = {
        repository: outcome
        for repository, outcome in outcomes.items()
        if outcome.startswith("error:")
    }
    if changed or errors:
        logger.info(
            "dockerhub_visibility_reconciled",
            extra={
                "target": "public" if public else "private",
                "competition_id": competition_id,
                "changed": changed,
                "errors": errors,
            },
        )
    else:
        logger.debug(
            "dockerhub_visibility_unchanged",
            extra={
                "target": "public" if public else "private",
                "competition_id": competition_id,
                "repositories": repositories,
            },
        )
    return outcomes


def _sleep_seconds(interval_seconds: float, *, now: datetime | None = None) -> float:
    """How long to wait before the next reconcile.

    The interval is the ceiling, not the cadence: when a window edge falls sooner the
    loop wakes just after it instead, so a repository opens on eval_starts_at rather
    than up to one interval later.
    """
    boundary = _NEXT_BOUNDARY
    if boundary is None:
        return interval_seconds
    now = now or datetime.now(timezone.utc)
    # A second past the edge, so the tick that runs there computes the phase the
    # boundary implies rather than racing it.
    until_boundary = (boundary - now).total_seconds() + 1.0
    return max(_MIN_SLEEP_SECONDS, min(interval_seconds, until_boundary))


async def _run_visibility_loop(interval_seconds: float) -> None:
    while True:
        try:
            await run_visibility_tick()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("dockerhub_visibility_tick_failed")
        try:
            await asyncio.sleep(_sleep_seconds(interval_seconds))
        except asyncio.CancelledError:
            raise


def start_dockerhub_visibility_task(app) -> None:
    if not bool(settings.dockerhub_visibility_enabled):
        logger.info("dockerhub_visibility_disabled")
        return
    repositories = [
        repository.strip()
        for repository in settings.dockerhub_task_repositories
        if repository.strip()
    ]
    if not repositories and not hf_task_sync.sync_enabled():
        logger.warning("dockerhub_visibility_no_repositories_configured")
        return

    interval = max(30.0, float(settings.dockerhub_visibility_interval_seconds))
    task = asyncio.create_task(_run_visibility_loop(interval))
    app.state.dockerhub_visibility_task = task
    # The dispatch gates are only armed once the loop that feeds them exists, so a
    # deployment without this loop dispatches SOMA runs as it did before instead of
    # waiting on a readiness snapshot nothing will ever publish.
    dockerhub_task_sync.arm_dispatch_gate()
    hf_task_sync.arm_dispatch_gate()
    logger.info(
        "dockerhub_visibility_started",
        extra={
            "interval_seconds": interval,
            "repositories": repositories,
            "public_from": str(settings.dockerhub_visibility_public_from),
            "task_sync_enabled": dockerhub_task_sync.sync_enabled(),
            "task_sync_source": dockerhub_task_sync.source_repository(),
            "task_sync_target": dockerhub_task_sync.target_repository(),
            "task_sync_prune": bool(settings.dockerhub_task_sync_prune),
            "task_sync_blocks_dispatch": bool(settings.dockerhub_task_sync_block_dispatch),
            "dataset_sync_enabled": hf_task_sync.sync_enabled(),
            "dataset_repository": hf_task_sync.repository(),
            "dataset_source_file": str(settings.hf_dataset_source_file),
            "dataset_prune": bool(settings.hf_dataset_prune),
            "dataset_blocks_dispatch": bool(settings.hf_dataset_block_dispatch),
        },
    )


async def stop_dockerhub_visibility_task(app) -> None:
    # Disarmed first: from here on nothing is going to refresh the readiness snapshots,
    # so the gates must not keep holding runs back on a stale one.
    dockerhub_task_sync.disarm_dispatch_gate()
    hf_task_sync.disarm_dispatch_gate()
    task = getattr(app.state, "dockerhub_visibility_task", None)
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.exception("dockerhub_visibility_stop_failed")
    app.state.dockerhub_visibility_task = None
    logger.info("dockerhub_visibility_stopped")
