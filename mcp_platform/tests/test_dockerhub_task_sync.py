"""Tests for deriving the competition task repository from swe_bench_tasks.

This sync is the only thing in the platform that *deletes* published artefacts, and it
feeds a dispatch gate, so the tests pin the two properties that make it safe rather
than the mechanics of copying:

* deletions happen only when the same tick leaves the repository private, so a prune
  can never run while a validator is pulling
* a task is dispatchable only once both of its images are actually in the repository
"""

from __future__ import annotations

import asyncio
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

from app.services import dockerhub_task_sync as sync  # noqa: E402

NOW = datetime(2026, 9, 10, tzinfo=timezone.utc)
SOURCE = "ns/source"
TARGET = "ns/target"

SOMA_BENCHMARK = "soma-is-tasks"
SWEBENCH_BENCHMARK = "SWE-bench/SWE-bench_Verified"


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch):
    sync.reset_snapshot()
    sync.arm_dispatch_gate()
    monkeypatch.setattr(sync.settings, "dockerhub_task_sync_enabled", True, raising=False)
    monkeypatch.setattr(sync.settings, "dockerhub_task_sync_prune", True, raising=False)
    monkeypatch.setattr(sync.settings, "dockerhub_task_sync_block_dispatch", True, raising=False)
    monkeypatch.setattr(sync.settings, "dockerhub_task_source_repository", SOURCE, raising=False)
    monkeypatch.setattr(sync.settings, "dockerhub_task_target_repository", TARGET, raising=False)
    yield
    sync.reset_snapshot()
    sync.disarm_dispatch_gate()


# ── desired state ──────────────────────────────────────────────────────────


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _FakeDb:
    def __init__(self, rows):
        self.rows = rows

    async def execute(self, _statement):
        return _FakeResult(self.rows)


def test_desired_ids_cover_soma_tasks_and_skip_swebench():
    """SWE-bench instances resolve from a public dataset, so nothing of theirs is here."""
    db = _FakeDb(
        [
            ("falconry__falcon-2673", SOMA_BENCHMARK, 2),
            ("django__django-13821", SWEBENCH_BENCHMARK, 1),
            ("  ", SOMA_BENCHMARK, None),
        ]
    )

    desired = asyncio.run(sync.load_desired_instance_ids(db, competition_ids={112}))

    assert desired == {"falconry__falcon-2673"}


def test_a_task_with_no_benchmark_recorded_is_resolved_like_dispatch_resolves_it():
    """The sync and the gate must agree on what a blank benchmark name means.

    Dispatch resolves an empty name to the stage default, which for stage 2 and full
    evaluation is a SOMA task list. If this filter read the raw column instead, such a
    task would be gated at dispatch but never copied - blocked forever, with nothing
    in the logs pointing at why.
    """
    db = _FakeDb([("eval-task", "", None), ("stage1-task", "", 1)])

    desired = asyncio.run(sync.load_desired_instance_ids(db, competition_ids={112}))

    assert desired == {"eval-task"}
    # The gate resolves the same way, so the two agree row by row.
    assert (
        sync.dispatch_block_reason(benchmark_name="", instance_id="eval-task", screener_stage=None)
        == sync.BLOCK_REASON_SYNC_PENDING
    )
    assert (
        sync.dispatch_block_reason(benchmark_name="", instance_id="stage1-task", screener_stage=1)
        is None
    )


def test_no_competitions_means_no_query():
    db = _FakeDb([("falconry__falcon-2673", SOMA_BENCHMARK, 2)])

    assert asyncio.run(sync.load_desired_instance_ids(db, competition_ids=set())) == set()


def test_relevant_competitions_include_the_not_yet_started():
    """Images have to be in place before the window opens, and stay while it is open."""
    windows = [
        (1, NOW - timedelta(days=10), NOW - timedelta(days=1)),  # closed
        (2, NOW - timedelta(days=1), NOW + timedelta(days=1)),  # open
        (3, NOW + timedelta(days=5), NOW + timedelta(days=9)),  # future
    ]

    assert sync.relevant_competition_ids(windows, now=NOW) == {2, 3}


def test_publishable_competitions_exclude_the_not_yet_started():
    """Narrower than relevant: what may be copied in while the repository is public."""
    windows = [
        (1, NOW - timedelta(days=10), NOW - timedelta(days=1)),  # closed
        (2, NOW - timedelta(days=1), NOW + timedelta(days=1)),  # open
        (3, NOW + timedelta(days=5), NOW + timedelta(days=9)),  # future
    ]

    assert sync.publishable_competition_ids(windows, now=NOW) == {2}


# ── plan ───────────────────────────────────────────────────────────────────


def test_plan_copies_both_tags_of_a_task():
    plan = sync.plan_tag_sync(desired_instance_ids={"task-a"}, present_tags=set(), prune=False)

    assert plan.to_copy == ("task-a", "task-a.test")
    assert plan.to_delete == ()


def test_plan_prunes_everything_the_database_does_not_ask_for():
    plan = sync.plan_tag_sync(
        desired_instance_ids={"task-a"},
        present_tags={"task-a", "task-a.test", "old-task", "old-task.test", "stray-tag"},
        prune=True,
    )

    assert plan.to_copy == ()
    # Including "stray-tag", which belongs to no competition at all: a repository that
    # keeps leftovers is not the exactly-this-competition set the window publishes.
    assert plan.to_delete == ("old-task", "old-task.test", "stray-tag")


def test_plan_without_prune_leaves_extra_tags_alone():
    plan = sync.plan_tag_sync(
        desired_instance_ids={"task-a"}, present_tags={"old-task"}, prune=False
    )

    assert plan.to_delete == ()


def test_plan_empties_the_repository_when_the_competition_has_no_tasks():
    """The start-of-competition case: nothing wanted yet, so nothing stays."""
    plan = sync.plan_tag_sync(
        desired_instance_ids=set(), present_tags={"old-task", "old-task.test"}, prune=True
    )

    assert plan.to_delete == ("old-task", "old-task.test")


def test_readiness_requires_both_images():
    ready = sync.ready_instance_ids(
        desired_instance_ids={"task-a", "task-b"},
        present_tags={"task-a", "task-a.test", "task-b"},
    )

    assert ready == {"task-a"}


# ── apply ──────────────────────────────────────────────────────────────────


class _FakeHub:
    DockerHubError = sync.hub.DockerHubError

    def __init__(self, *, tags: set[str], source_tags: set[str], exists: bool = True):
        self.tags = set(tags)
        self.source_tags = set(source_tags)
        self.exists = exists
        self.calls: list[tuple[str, str]] = []

    def login(self):
        return "jwt"

    def create_private_repository(self, repository, *, jwt, description):
        if self.exists:
            return False
        self.exists = True
        self.calls.append(("create", repository))
        return True

    def list_tag_names(self, repository, *, jwt):
        return set(self.tags)

    def copy_scope_token(self, *, source, target):
        return "token"

    def copy_tag(self, *, source, target, tag, token):
        self.calls.append(("copy", tag))
        if tag not in self.source_tags:
            return "missing-in-source"
        self.tags.add(tag)
        return "copied"

    def delete_tag(self, repository, tag, *, jwt):
        self.calls.append(("delete", tag))
        self.tags.discard(tag)


def _install(monkeypatch, fake: _FakeHub) -> None:
    monkeypatch.setattr(sync, "hub", fake)


def test_apply_copies_before_it_deletes(monkeypatch):
    """Order matters when a tick both adds and removes: never leave a gap."""
    fake = _FakeHub(tags={"old-task", "old-task.test"}, source_tags={"new-task", "new-task.test"})
    _install(monkeypatch, fake)

    result = sync._apply_tag_sync(
        source=SOURCE, target=TARGET, desired_instance_ids={"new-task"}, prune=True
    )

    assert [call[0] for call in fake.calls] == ["copy", "copy", "delete", "delete"]
    assert result["deleted"] == ["old-task", "old-task.test"]
    assert result["present_tags"] == {"new-task", "new-task.test"}


def test_apply_creates_the_repository_private_when_it_is_missing(monkeypatch):
    fake = _FakeHub(tags=set(), source_tags={"task-a", "task-a.test"}, exists=False)
    _install(monkeypatch, fake)

    result = sync._apply_tag_sync(
        source=SOURCE, target=TARGET, desired_instance_ids={"task-a"}, prune=True
    )

    assert result["created_repository"] is True
    assert ("create", TARGET) in fake.calls


def test_apply_reports_a_task_missing_from_the_source_and_keeps_going(monkeypatch):
    fake = _FakeHub(tags=set(), source_tags={"task-b", "task-b.test"})
    _install(monkeypatch, fake)

    result = sync._apply_tag_sync(
        source=SOURCE, target=TARGET, desired_instance_ids={"task-a", "task-b"}, prune=False
    )

    assert set(result["errors"]) == {"task-a", "task-a.test"}
    assert result["copied"] == {"task-b": "copied", "task-b.test": "copied"}


def test_apply_survives_a_failing_copy(monkeypatch):
    fake = _FakeHub(tags=set(), source_tags={"task-a", "task-a.test", "task-b", "task-b.test"})

    def _copy_tag(*, source, target, tag, token):
        if tag.startswith("task-a"):
            raise sync.hub.DockerHubError("boom")
        return _FakeHub.copy_tag(fake, source=source, target=target, tag=tag, token=token)

    fake.copy_tag = _copy_tag
    _install(monkeypatch, fake)

    result = sync._apply_tag_sync(
        source=SOURCE, target=TARGET, desired_instance_ids={"task-a", "task-b"}, prune=False
    )

    assert set(result["errors"]) == {"task-a", "task-a.test"}
    assert "task-b" in result["copied"]


# ── tick ───────────────────────────────────────────────────────────────────


def _run_tick(monkeypatch, fake: _FakeHub, *, desired: set[str], public: bool):
    _install(monkeypatch, fake)

    async def _desired(db, *, competition_ids):
        return set(desired)

    monkeypatch.setattr(sync, "load_desired_instance_ids", _desired)
    windows = [(1, NOW - timedelta(days=1), NOW + timedelta(days=1))]
    return asyncio.run(
        sync.run_task_sync_tick(db=None, windows=windows, public=public, now=NOW)
    )


def _selected_competition_ids(monkeypatch, *, windows, public) -> set[int]:
    """Which competitions a tick would copy images for, captured at the DB boundary."""
    _install(monkeypatch, _FakeHub(tags=set(), source_tags=set()))
    seen: dict[str, set[int]] = {}

    async def _desired(db, *, competition_ids):
        seen["ids"] = set(competition_ids)
        return set()

    monkeypatch.setattr(sync, "load_desired_instance_ids", _desired)
    asyncio.run(sync.run_task_sync_tick(db=None, windows=windows, public=public, now=NOW))
    return seen["ids"]


def test_a_public_tick_does_not_copy_in_the_next_competitions_tasks(monkeypatch):
    """A handover must not put the next competition's hidden images in a public repo.

    Competition 1 is published; competition 2 is already configured but its own window
    has not opened. Its images are staged on the next private tick instead.
    """
    windows = [
        (1, NOW - timedelta(days=1), NOW + timedelta(days=1)),
        (2, NOW + timedelta(days=5), NOW + timedelta(days=9)),
    ]

    assert _selected_competition_ids(monkeypatch, windows=windows, public=True) == {1}


def test_a_private_tick_stages_the_next_competitions_tasks(monkeypatch):
    windows = [
        (1, NOW - timedelta(days=1), NOW + timedelta(days=1)),
        (2, NOW + timedelta(days=5), NOW + timedelta(days=9)),
    ]

    assert _selected_competition_ids(monkeypatch, windows=windows, public=False) == {1, 2}


def test_tick_does_not_delete_while_the_repository_is_public(monkeypatch):
    """The invariant: a prune while validators are pulling would break grading."""
    fake = _FakeHub(tags={"old-task", "old-task.test"}, source_tags={"task-a", "task-a.test"})

    summary = _run_tick(monkeypatch, fake, desired={"task-a"}, public=True)

    assert summary["pruned"] is False
    assert summary["deleted"] == []
    assert "old-task" in fake.tags


def test_tick_deletes_while_the_repository_is_private(monkeypatch):
    fake = _FakeHub(tags={"old-task", "old-task.test"}, source_tags={"task-a", "task-a.test"})

    summary = _run_tick(monkeypatch, fake, desired={"task-a"}, public=False)

    assert summary["pruned"] is True
    assert summary["deleted"] == ["old-task", "old-task.test"]


def test_tick_publishes_readiness_for_the_dispatch_gate(monkeypatch):
    fake = _FakeHub(tags=set(), source_tags={"task-a", "task-a.test", "task-b"})

    summary = _run_tick(monkeypatch, fake, desired={"task-a", "task-b"}, public=False)

    snapshot = sync.current_snapshot()
    assert snapshot is not None
    assert snapshot.ready_instance_ids == frozenset({"task-a"})
    # task-b has an env image but no grading image, so it is not runnable.
    assert snapshot.pending_instance_ids == frozenset({"task-b"})
    assert summary["ready_tasks"] == 1


def test_tick_requires_both_repositories_configured(monkeypatch):
    monkeypatch.setattr(sync.settings, "dockerhub_task_target_repository", "", raising=False)
    monkeypatch.setattr(sync.settings, "dockerhub_task_repositories", [], raising=False)

    with pytest.raises(sync.TaskSyncError):
        asyncio.run(sync.run_task_sync_tick(db=None, windows=[], public=False, now=NOW))


def test_target_falls_back_to_the_visibility_list(monkeypatch):
    monkeypatch.setattr(sync.settings, "dockerhub_task_target_repository", None, raising=False)
    monkeypatch.setattr(sync.settings, "dockerhub_task_repositories", ["ns/from-list"], raising=False)

    assert sync.target_repository() == "ns/from-list"


# ── dispatch gate ──────────────────────────────────────────────────────────


def test_gate_blocks_before_the_first_sync_has_run():
    """Unknown readiness must read as "not yet": the repository may still hold the
    previous competition's tags."""
    assert (
        sync.dispatch_block_reason(benchmark_name=SOMA_BENCHMARK, instance_id="task-a")
        == sync.BLOCK_REASON_SYNC_PENDING
    )


def test_gate_blocks_a_task_whose_images_are_not_there_yet(monkeypatch):
    fake = _FakeHub(tags=set(), source_tags={"task-a", "task-a.test"})
    _run_tick(monkeypatch, fake, desired={"task-a"}, public=False)

    assert sync.dispatch_block_reason(benchmark_name=SOMA_BENCHMARK, instance_id="task-a") is None
    assert (
        sync.dispatch_block_reason(benchmark_name=SOMA_BENCHMARK, instance_id="task-z")
        == sync.BLOCK_REASON_IMAGES_MISSING
    )


def test_gate_never_holds_back_a_swebench_task():
    """Stage-1 tasks come from the public dataset - this repository is irrelevant to
    them, and gating them would stall screening."""
    assert sync.dispatch_block_reason(benchmark_name=SWEBENCH_BENCHMARK, instance_id="x") is None


def test_gate_is_inert_when_the_sync_is_disabled(monkeypatch):
    """Turning the sync off must not freeze dispatch - images are then managed by hand."""
    monkeypatch.setattr(sync.settings, "dockerhub_task_sync_enabled", False, raising=False)

    assert sync.dispatch_block_reason(benchmark_name=SOMA_BENCHMARK, instance_id="task-a") is None


def test_gate_can_be_disabled_on_its_own(monkeypatch):
    monkeypatch.setattr(sync.settings, "dockerhub_task_sync_block_dispatch", False, raising=False)

    assert sync.dispatch_block_reason(benchmark_name=SOMA_BENCHMARK, instance_id="task-a") is None


def test_gate_stays_open_when_no_reconcile_loop_is_running():
    """The visibility loop can be switched off, or fail to start (main.py treats that
    as non-fatal). Nothing would then ever publish a readiness snapshot, so an armed
    gate would hold every SOMA run in `pending` for good."""
    sync.disarm_dispatch_gate()

    assert sync.dispatch_block_reason(benchmark_name=SOMA_BENCHMARK, instance_id="task-a") is None


def test_stopping_the_loop_disarms_the_gate(monkeypatch):
    from app.services import dockerhub_visibility as visibility

    class _AppState:
        pass

    class _App:
        state = _AppState()

    asyncio.run(visibility.stop_dockerhub_visibility_task(_App()))

    assert sync.dispatch_gate_armed() is False
