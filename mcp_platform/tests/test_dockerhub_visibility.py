"""Tests for the hidden-task repository visibility window.

The consequence of getting this wrong runs both ways: too late and validators cannot
pull the images they grade with, too early and a competition's hidden tasks are
published while miners are still being scored on them. So the window boundaries, the
"no active competition means private" default, and the reconcile-not-react behaviour
are all pinned here.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

TESTS_DIR = os.path.dirname(__file__)
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if MCP_PLATFORM_DIR not in sys.path:
    sys.path.insert(0, MCP_PLATFORM_DIR)

os.environ["DEBUG"] = "false"
os.environ.setdefault("PRIVATE_NETWORK_CIDRS", "[]")
os.environ.setdefault("TRUSTED_PROXY_CIDRS", "[]")
os.environ.setdefault("SANDBOX_SERVICE_URL", "http://localhost")

from app.services import dockerhub_visibility  # noqa: E402
from soma_shared.db.models.competition_timeframe import CompetitionTimeframe  # noqa: E402

UPLOAD_STARTS = datetime(2026, 9, 1, tzinfo=timezone.utc)
UPLOAD_ENDS = datetime(2026, 9, 8, tzinfo=timezone.utc)
EVAL_STARTS = datetime(2026, 9, 10, tzinfo=timezone.utc)
EVAL_ENDS = datetime(2026, 9, 15, tzinfo=timezone.utc)


def _timeframe() -> CompetitionTimeframe:
    return CompetitionTimeframe(
        upload_starts_at=UPLOAD_STARTS,
        upload_ends_at=UPLOAD_ENDS,
        eval_starts_at=EVAL_STARTS,
        eval_ends_at=EVAL_ENDS,
    )


def test_window_opens_at_eval_start_by_default():
    """eval_starts_at is when the first hidden-task run can be dispatched.

    Screener stage 2 seeding is gated on ``now >= eval_starts_at``, and full evaluation
    follows it, so this default window covers every run that needs a task image - there
    is no earlier phase left uncovered.
    """
    start, end = dockerhub_visibility.public_window_for_timeframe(
        _timeframe(),
        public_from=dockerhub_visibility.PUBLIC_FROM_EVAL_START,
        grace_seconds=0.0,
    )

    assert start == EVAL_STARTS
    assert end == EVAL_ENDS


def test_window_can_open_before_stage_2_instead():
    """upload_ends_at opens the window before stage 2 begins, not at its start.

    Stage 2 is seeded from eval_starts_at, so this option only adds lead-in over the
    idle stretch - enough that the repository is already public when the first stage-2
    run is dispatched, rather than up to one reconcile tick later.
    """
    start, end = dockerhub_visibility.public_window_for_timeframe(
        _timeframe(),
        public_from=dockerhub_visibility.PUBLIC_FROM_UPLOAD_END,
        grace_seconds=0.0,
    )

    assert start == UPLOAD_ENDS
    assert end == EVAL_ENDS


def test_grace_extends_the_window_past_eval_end():
    _start, end = dockerhub_visibility.public_window_for_timeframe(
        _timeframe(),
        public_from=dockerhub_visibility.PUBLIC_FROM_EVAL_START,
        grace_seconds=4 * 3600.0,
    )

    assert end == EVAL_ENDS + timedelta(hours=4)


def test_naive_timeframe_datetimes_are_read_as_utc():
    """Postgres can hand back naive datetimes; comparing them to an aware `now` would
    raise rather than simply mis-order."""
    timeframe = CompetitionTimeframe(
        upload_starts_at=UPLOAD_STARTS.replace(tzinfo=None),
        upload_ends_at=UPLOAD_ENDS.replace(tzinfo=None),
        eval_starts_at=EVAL_STARTS.replace(tzinfo=None),
        eval_ends_at=EVAL_ENDS.replace(tzinfo=None),
    )

    start, end = dockerhub_visibility.public_window_for_timeframe(
        timeframe,
        public_from=dockerhub_visibility.PUBLIC_FROM_EVAL_START,
        grace_seconds=0.0,
    )

    assert start == EVAL_STARTS
    assert end == EVAL_ENDS


@pytest.mark.parametrize(
    "now, expected_public",
    [
        (UPLOAD_STARTS, False),
        (UPLOAD_ENDS, False),
        (EVAL_STARTS - timedelta(seconds=1), False),
        (EVAL_STARTS, True),
        (EVAL_ENDS - timedelta(seconds=1), True),
        (EVAL_ENDS, False),
        (EVAL_ENDS + timedelta(days=30), False),
    ],
)
def test_public_only_inside_the_window(now, expected_public):
    windows = [(7, EVAL_STARTS, EVAL_ENDS)]

    public, competition_id = dockerhub_visibility.should_be_public(windows, now=now)

    assert public is expected_public
    assert competition_id == (7 if expected_public else None)


def test_no_active_competition_means_private():
    """The safe default: nothing configured must never leave images published."""
    public, competition_id = dockerhub_visibility.should_be_public([], now=EVAL_STARTS)

    assert public is False
    assert competition_id is None


def test_any_overlapping_competition_keeps_it_public():
    """Competitions overlap at a handover, and the later one still needs the images."""
    windows = [
        (1, EVAL_STARTS, EVAL_ENDS),
        (2, EVAL_ENDS, EVAL_ENDS + timedelta(days=5)),
    ]

    public, competition_id = dockerhub_visibility.should_be_public(
        windows, now=EVAL_ENDS + timedelta(days=1)
    )

    assert public is True
    assert competition_id == 2


# ── reconcile ──────────────────────────────────────────────────────────────


class _FakeHub:
    def __init__(self, is_private: dict[str, bool]):
        self.state = dict(is_private)
        self.calls: list[tuple[str, bool]] = []

    def login(self) -> str:
        return "jwt"

    def is_private(self, repository, *, jwt):
        return self.state[repository]

    def set_private(self, repository, *, jwt, private):
        self.calls.append((repository, private))
        self.state[repository] = private
        return private


def _install_fake_hub(monkeypatch, fake: _FakeHub) -> None:
    monkeypatch.setattr(dockerhub_visibility, "_login", fake.login)
    monkeypatch.setattr(dockerhub_visibility, "_is_private", fake.is_private)
    monkeypatch.setattr(dockerhub_visibility, "_set_private", fake.set_private)


def test_reconcile_only_changes_a_repository_that_disagrees(monkeypatch):
    fake = _FakeHub({"ns/a": True, "ns/b": False})
    _install_fake_hub(monkeypatch, fake)

    outcomes = dockerhub_visibility._reconcile_repositories(["ns/a", "ns/b"], public=True)

    # ns/b is already public, so it is left alone rather than called again.
    assert fake.calls == [("ns/a", False)]
    assert outcomes == {"ns/a": "changed_to_public", "ns/b": "public"}


def test_reconcile_makes_repositories_private_again(monkeypatch):
    fake = _FakeHub({"ns/a": False})
    _install_fake_hub(monkeypatch, fake)

    outcomes = dockerhub_visibility._reconcile_repositories(["ns/a"], public=False)

    assert fake.calls == [("ns/a", True)]
    assert outcomes == {"ns/a": "changed_to_private"}


def test_reconcile_reports_a_failure_per_repository_and_keeps_going(monkeypatch):
    """One unreachable repository must not stop the others from being reconciled."""
    fake = _FakeHub({"ns/b": True})

    def _is_private(repository, *, jwt):
        if repository == "ns/a":
            raise dockerhub_visibility.DockerHubVisibilityError("boom")
        return fake.state[repository]

    monkeypatch.setattr(dockerhub_visibility, "_login", fake.login)
    monkeypatch.setattr(dockerhub_visibility, "_is_private", _is_private)
    monkeypatch.setattr(dockerhub_visibility, "_set_private", fake.set_private)

    outcomes = dockerhub_visibility._reconcile_repositories(["ns/a", "ns/b"], public=True)

    assert outcomes["ns/a"].startswith("error:")
    assert outcomes["ns/b"] == "changed_to_public"


def test_login_requires_credentials(monkeypatch):
    monkeypatch.setattr(dockerhub_visibility.settings, "dockerhub_username", "", raising=False)
    monkeypatch.setattr(dockerhub_visibility.settings, "dockerhub_token", "", raising=False)

    with pytest.raises(dockerhub_visibility.DockerHubVisibilityError, match="DOCKERHUB_USERNAME"):
        dockerhub_visibility._login()


def test_repository_must_be_namespaced():
    with pytest.raises(dockerhub_visibility.DockerHubVisibilityError, match="namespace"):
        dockerhub_visibility._split_repository("no-namespace")

    assert dockerhub_visibility._split_repository("ns/name") == ("ns", "name")

def test_reconcile_errors_when_the_repository_does_not_actually_move(monkeypatch):
    """A request the API accepts but ignores must not be reported as a flip.

    Docker Hub's repository PATCH endpoint behaves exactly like this - it echoes back
    the is_private it was given without applying it - which is why the visibility
    change is verified by reading the repository back.
    """

    class _IgnoringHub(_FakeHub):
        def set_private(self, repository, *, jwt, private):
            self.calls.append((repository, private))
            return self.state[repository]  # unchanged

    fake = _IgnoringHub({"ns/a": True})
    _install_fake_hub(monkeypatch, fake)

    outcomes = dockerhub_visibility._reconcile_repositories(["ns/a"], public=True)

    assert outcomes["ns/a"].startswith("error: visibility unchanged")
    assert "wanted public" in outcomes["ns/a"]
