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
                                                             |----- task images public ------|  + grace

So ``eval_starts_at`` (the default) already covers every hidden-task run: the images go
public exactly when the first stage-2 run can be dispatched, not after stage 2.

``DOCKERHUB_VISIBILITY_PUBLIC_FROM=upload_ends_at`` moves the opening back into the idle
stretch. That grades nothing extra - there is nothing to grade there yet - and its only
purpose is to remove the race at the boundary: this loop reconciles on an interval
(``DOCKERHUB_VISIBILITY_INTERVAL_SECONDS``), so at ``eval_starts_at`` the repository can
still be private for up to one tick, and the first validations to claim a stage-2 run
would fail to pull and retry. Opening early trades a longer exposure window for removing
that hiccup.

It closes at ``eval_ends_at`` plus ``DOCKERHUB_VISIBILITY_GRACE_SECONDS``, so a
validation still in flight when the competition ends can finish pulling.

This reconciles rather than reacting to events: every tick computes the visibility the
current time implies and only calls Docker Hub when it disagrees. A missed tick, a
restart, or a manual change in the Docker Hub UI therefore self-corrects, and no state
has to be persisted anywhere.
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logging import get_logger
from soma_shared.db.models.competition_config import CompetitionConfig
from soma_shared.db.models.competition_timeframe import CompetitionTimeframe
from soma_shared.db.session import get_db_session

logger = get_logger(__name__)

HUB_API_BASE = "https://hub.docker.com/v2"
_LOGIN_PATH = "/users/login/"

PUBLIC_FROM_EVAL_START = "eval_starts_at"
PUBLIC_FROM_UPLOAD_END = "upload_ends_at"


class DockerHubVisibilityError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# window computation
# ---------------------------------------------------------------------------


def _as_utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def public_window_for_timeframe(
    timeframe: CompetitionTimeframe,
    *,
    public_from: str,
    grace_seconds: float,
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
    end = _as_utc(timeframe.eval_ends_at) + timedelta(seconds=max(0.0, grace_seconds))
    return start, end


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
    grace_seconds = float(settings.dockerhub_visibility_grace_seconds)

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
            grace_seconds=grace_seconds,
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
# Docker Hub API
# ---------------------------------------------------------------------------


def _hub_call(
    path: str,
    *,
    method: str = "GET",
    body: dict | None = None,
    jwt: str | None = None,
) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        f"{HUB_API_BASE}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    if jwt:
        request.add_header("Authorization", f"Bearer {jwt}")
    timeout = float(settings.dockerhub_api_timeout_seconds)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise DockerHubVisibilityError(
            f"{method} {path} failed ({exc.code}): {detail}"
        ) from exc
    except urllib.error.URLError as exc:
        raise DockerHubVisibilityError(f"{method} {path} failed: {exc.reason}") from exc
    return json.loads(raw) if raw else {}


def _login() -> str:
    username = (settings.dockerhub_username or "").strip()
    token = (settings.dockerhub_token or "").strip()
    if not username or not token:
        raise DockerHubVisibilityError(
            "DOCKERHUB_USERNAME and DOCKERHUB_TOKEN are required for visibility management"
        )
    # A personal access token is exchanged for a short-lived JWT the same way the
    # docker CLI does it; the token itself is never sent to the repository endpoints.
    payload = _hub_call(
        _LOGIN_PATH,
        method="POST",
        body={"username": username, "password": token},
    )
    jwt = str(payload.get("token") or "")
    if not jwt:
        raise DockerHubVisibilityError("Docker Hub login returned no token")
    return jwt


def _split_repository(repository: str) -> tuple[str, str]:
    if "/" not in repository:
        raise DockerHubVisibilityError(
            f"repository must be '<namespace>/<name>', got {repository!r}"
        )
    namespace, name = repository.split("/", 1)
    return namespace.strip(), name.strip()


def _is_private(repository: str, *, jwt: str) -> bool:
    namespace, name = _split_repository(repository)
    payload = _hub_call(f"/repositories/{namespace}/{name}/", jwt=jwt)
    return bool(payload.get("is_private"))


def _set_private(repository: str, *, jwt: str, private: bool) -> bool:
    """Set the repository's visibility and return what it actually became.

    Visibility is changed through the dedicated ``privacy/`` endpoint, and the body
    must always carry an explicit ``is_private`` boolean. Both details were verified
    against the live API and both matter:

    * ``PATCH /v2/repositories/{ns}/{repo}/`` accepts an ``is_private`` field and
      echoes it back in its response, but does not apply it - it silently leaves the
      repository as it was. Trusting that response reports a change that never
      happened.
    * The ``privacy/`` endpoint treats a body without a recognised ``is_private`` key
      as a request to make the repository PUBLIC. A typo or a renamed field therefore
      fails in the one direction that leaks hidden tasks, which is why the value is
      sent as an explicit boolean and never as a status string.

    The endpoint returns an empty body either way, so the result is read back from the
    repository itself rather than inferred from the request having succeeded.
    """
    namespace, name = _split_repository(repository)
    _hub_call(
        f"/repositories/{namespace}/{name}/privacy/",
        method="POST",
        jwt=jwt,
        body={"is_private": bool(private)},
    )
    return _is_private(repository, jwt=jwt)


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


# ---------------------------------------------------------------------------
# reconcile loop
# ---------------------------------------------------------------------------


async def run_visibility_tick(now: datetime | None = None) -> dict[str, str]:
    """Bring every configured repository in line with the current competition phase."""
    repositories = [
        repository.strip()
        for repository in settings.dockerhub_task_repositories
        if repository.strip()
    ]
    if not repositories:
        return {}

    now = now or datetime.now(timezone.utc)
    windows: list[tuple[int, datetime, datetime]] = []
    async for db in get_db_session():
        windows = await load_active_public_windows(db)
        break

    public, competition_id = should_be_public(windows, now=now)
    outcomes = await asyncio.to_thread(_reconcile_repositories, repositories, public=public)

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


async def _run_visibility_loop(interval_seconds: float) -> None:
    while True:
        try:
            await run_visibility_tick()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("dockerhub_visibility_tick_failed")
        try:
            await asyncio.sleep(interval_seconds)
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
    if not repositories:
        logger.warning("dockerhub_visibility_no_repositories_configured")
        return

    interval = max(30.0, float(settings.dockerhub_visibility_interval_seconds))
    task = asyncio.create_task(_run_visibility_loop(interval))
    app.state.dockerhub_visibility_task = task
    logger.info(
        "dockerhub_visibility_started",
        extra={
            "interval_seconds": interval,
            "repositories": repositories,
            "public_from": str(settings.dockerhub_visibility_public_from),
            "grace_seconds": float(settings.dockerhub_visibility_grace_seconds),
        },
    )


async def stop_dockerhub_visibility_task(app) -> None:
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
