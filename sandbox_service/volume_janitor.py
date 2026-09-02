"""Safety-net logic that removes orphaned Docker volumes.

Why this exists: soma-copilot runs create several volumes per run (named
workspace/home/certs volumes, plus anonymous volumes Docker auto-creates for
images that declare VOLUME, e.g. docker:dind's /var/lib/docker). Normal
teardown (`docker compose down --volumes` + the app's own retried
`docker volume rm`) sometimes fails to remove them - most commonly because the
privileged `dind` sidecar hasn't released its mount yet when removal is
attempted (see the raised `stop_grace_period` in the copilot compose file for
the other half of the mitigation). When that happens, nothing ever retries,
and the volume (and the RAM behind it, since volumes live on a tmpfs) is
leaked permanently.

This module is a generic, application-agnostic safety net: `sweep()` finds
every Docker volume that is dangling (attached to zero containers) and old
enough that it can no longer plausibly be a workspace still being set up, and
removes it. It never touches a volume that is in use by a running container -
`docker volume rm` refuses that regardless.

Imported and run on a background thread inside sandbox_service's own process
(see main.py's startup hook) rather than as a separate pm2 process, so it
shares the service's lifecycle and logging config. Does not configure
logging itself - relies on the host process (main.py) having done so.
"""
from __future__ import annotations

import logging
import subprocess
import time
from datetime import datetime

# How often to sweep for orphaned volumes.
SWEEP_INTERVAL_SECONDS = 120

MIN_AGE_SECONDS = 3600

logger = logging.getLogger(__name__)


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=30)


def _parse_docker_timestamp(raw: str) -> float | None:
    if not raw:
        return None
    normalized = raw.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        return datetime.fromisoformat(normalized).timestamp()
    except ValueError:
        return None


def sweep() -> None:
    listing = _run(["docker", "volume", "ls", "-f", "dangling=true", "-q"])
    if listing.returncode != 0:
        logger.warning("failed to list dangling volumes: %s", (listing.stderr or "").strip())
        return

    names = [n.strip() for n in listing.stdout.splitlines() if n.strip()]
    if not names:
        return

    now = time.time()
    removed = 0
    skipped_young = 0

    for name in names:
        inspect = _run(["docker", "volume", "inspect", "-f", "{{.CreatedAt}}", name])
        if inspect.returncode != 0:
            continue  # already gone (removed by the app itself, or by us on a prior pass)

        created = _parse_docker_timestamp(inspect.stdout)
        if created is None:
            continue  # unparseable timestamp - be conservative, leave it for next pass

        age_seconds = now - created
        if age_seconds < MIN_AGE_SECONDS:
            skipped_young += 1
            continue

        rm = _run(["docker", "volume", "rm", "-f", name])
        if rm.returncode == 0:
            removed += 1
        else:
            # Most commonly "volume is in use" - a container attached to it
            # between our dangling-list check and now. Leave it for next pass.
            logger.warning("failed to remove volume %s: %s", name, (rm.stderr or "").strip())

    if removed or skipped_young:
        logger.info(
            "sweep done: removed=%d skipped_too_young=%d (min_age=%ds)",
            removed, skipped_young, MIN_AGE_SECONDS,
        )
