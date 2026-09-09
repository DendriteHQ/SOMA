"""Tests for publishing the competition's task rows into the Hugging Face dataset.

Like the image sync this is a mechanism that *removes* published artefacts and feeds a
dispatch gate, so the tests pin the properties that make it safe rather than the
mechanics of committing:

* rows are removed only when the same tick leaves the repository private
* while the repository is public, only competitions whose own window is open may be
  added - a handover must not publish the next competition's hidden tasks
* an unreadable source file aborts the tick instead of being read as "publish nothing"
* an unchanged row set produces no commit, so the repository does not grow a commit
  per tick
* a task is dispatchable only once its row is actually published
"""

from __future__ import annotations

import asyncio
import json
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

from app.services import hf_task_sync as sync  # noqa: E402

NOW = datetime(2026, 9, 10, tzinfo=timezone.utc)
REPO = "ns/dataset"
PATH = "tasks.jsonl"

SOMA_BENCHMARK = "soma-is-tasks"
SWEBENCH_BENCHMARK = "SWE-bench/SWE-bench_Verified"


def _row(instance_id: str, *, statement: str = "problem") -> dict:
    return {
        "instance_id": instance_id,
        "benchmark_name": SOMA_BENCHMARK,
        "problem_statement": statement,
        "FAIL_TO_PASS": ["tests/test_a.py::test_one"],
        "PASS_TO_PASS": [],
        "images": {"test": {"ref": f"ns/repo:{instance_id}.test"}},
    }


def _jsonl(*rows: dict) -> bytes:
    return "".join(json.dumps(row) + "\n" for row in rows).encode()


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch):
    sync.reset_snapshot()
    sync.arm_dispatch_gate()
    monkeypatch.setattr(sync.settings, "hf_dataset_enabled", True, raising=False)
    monkeypatch.setattr(sync.settings, "hf_dataset_prune", True, raising=False)
    monkeypatch.setattr(sync.settings, "hf_dataset_squash_on_prune", True, raising=False)
    monkeypatch.setattr(sync.settings, "hf_dataset_block_dispatch", True, raising=False)
    monkeypatch.setattr(sync.settings, "hf_dataset_repository", REPO, raising=False)
    monkeypatch.setattr(sync.settings, "hf_dataset_target_path", PATH, raising=False)
    yield
    sync.reset_snapshot()
    sync.disarm_dispatch_gate()


# ── rows ───────────────────────────────────────────────────────────────────


def test_parse_rows_keys_by_instance_id_and_drops_unusable_lines():
    payload = "\n".join(
        [
            json.dumps(_row("a")),
            "not json at all",
            json.dumps({"problem_statement": "no instance id"}),
            json.dumps(["not an object"]),
            "",
            json.dumps(_row("b")),
        ]
    )
    assert sorted(sync.parse_rows(payload)) == ["a", "b"]


def test_serialize_rows_is_stable_under_ordering():
    """"Did anything change" is a byte comparison, so the bytes cannot depend on order."""
    first = sync.serialize_rows({"b": _row("b"), "a": _row("a")})
    second = sync.serialize_rows({"a": _row("a"), "b": _row("b")})
    assert first == second
    assert sync.parse_rows(first).keys() == {"a", "b"}


def test_serialize_rows_round_trips_through_parse():
    rows = {"a": _row("a"), "b": _row("b", statement="other")}
    assert sync.parse_rows(sync.serialize_rows(rows)) == rows


def test_unreadable_source_file_raises_instead_of_publishing_nothing(monkeypatch, tmp_path):
    """An empty row set empties the dataset, so it must never come from a missing file."""
    missing = tmp_path / "absent.jsonl"
    monkeypatch.setattr(sync.settings, "hf_dataset_source_file", str(missing), raising=False)
    with pytest.raises(sync.TaskDatasetSyncError):
        sync.load_source_rows()


def test_source_file_is_resolved_against_the_repository_root(monkeypatch):
    monkeypatch.setattr(
        sync.settings, "hf_dataset_source_file", "tasks/soma_tasks.jsonl", raising=False
    )
    assert sync.source_file().is_absolute()
    assert sync.source_file().parts[-2:] == ("tasks", "soma_tasks.jsonl")


# ── planning ───────────────────────────────────────────────────────────────


def test_prune_publishes_exactly_the_desired_rows():
    plan = sync.plan_dataset_sync(
        desired={"a": _row("a")},
        published={"a": _row("a"), "old": _row("old")},
        prune=True,
    )
    assert set(plan.rows) == {"a"}
    assert plan.removed == ("old",)
    assert plan.unchanged is False


def test_without_prune_the_published_set_never_shrinks():
    plan = sync.plan_dataset_sync(
        desired={"a": _row("a")},
        published={"old": _row("old")},
        prune=False,
    )
    assert set(plan.rows) == {"a", "old"}
    assert plan.removed == ()
    assert plan.added == ("a",)


def test_a_corrected_row_still_lands_without_prune():
    """The desired row wins for an id in both, so a fixed row publishes while public."""
    plan = sync.plan_dataset_sync(
        desired={"a": _row("a", statement="fixed")},
        published={"a": _row("a", statement="broken")},
        prune=False,
    )
    assert plan.rows["a"]["problem_statement"] == "fixed"
    assert plan.updated == ("a",)
    assert plan.unchanged is False


def test_identical_rows_are_reported_unchanged():
    plan = sync.plan_dataset_sync(
        desired={"a": _row("a")}, published={"a": _row("a")}, prune=True
    )
    assert plan.unchanged is True
    assert (plan.added, plan.updated, plan.removed) == ((), (), ())


# ── competition selection ──────────────────────────────────────────────────


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _FakeDb:
    def __init__(self, rows):
        self.rows = rows
        self.statements = []

    async def execute(self, statement):
        self.statements.append(statement)
        return _FakeResult(self.rows)


def _windows(*specs):
    """``(competition_id, starts_in, ends_in)`` as hour offsets from NOW."""
    return [
        (competition_id, NOW + timedelta(hours=start), NOW + timedelta(hours=end))
        for competition_id, start, end in specs
    ]


def _selected_competition_ids(*, windows, public, monkeypatch) -> set[int]:
    """Which competitions a tick would select rows from, captured at the DB boundary."""
    seen: dict[str, set[int]] = {}

    async def _load(_db, *, competition_ids):
        seen["ids"] = set(competition_ids)
        return set()

    monkeypatch.setattr(sync.image_sync, "load_desired_instance_ids", _load)
    asyncio.run(
        sync.load_desired_rows(
            _FakeDb([]), windows=windows, now=NOW, public=public, source_rows={}
        )
    )
    return seen["ids"]


def test_public_tick_does_not_add_a_competition_that_is_only_being_prepared(monkeypatch):
    """A handover must not publish the next competition's hidden tasks.

    Competition 1 is published; competition 2 is already configured but its own window
    has not opened. Its rows may be staged - but not while the repository is public.
    """
    windows = _windows((1, -2, 2), (2, 5, 10))
    assert _selected_competition_ids(windows=windows, public=True, monkeypatch=monkeypatch) == {1}


def test_private_tick_stages_every_relevant_competition(monkeypatch):
    windows = _windows((1, -2, 2), (2, 5, 10))
    assert _selected_competition_ids(
        windows=windows, public=False, monkeypatch=monkeypatch
    ) == {1, 2}


def test_no_tick_selects_a_competition_whose_window_has_closed(monkeypatch):
    windows = _windows((0, -20, -10), (1, -2, 2))
    for public in (True, False):
        assert 0 not in _selected_competition_ids(
            windows=windows, public=public, monkeypatch=monkeypatch
        )


def test_public_tick_with_no_open_window_selects_nothing():
    db = _FakeDb([("future-task", SOMA_BENCHMARK, 2)])
    windows = _windows((2, 5, 10))
    desired, missing = asyncio.run(
        sync.load_desired_rows(
            db,
            windows=windows,
            now=NOW,
            public=True,
            source_rows={"future-task": _row("future-task")},
        )
    )
    assert desired == {}
    assert missing == set()


def test_swebench_tasks_are_never_published():
    db = _FakeDb(
        [
            ("soma-task", SOMA_BENCHMARK, 2),
            ("django__django-13821", SWEBENCH_BENCHMARK, 1),
        ]
    )
    source = {
        "soma-task": _row("soma-task"),
        "django__django-13821": _row("django__django-13821"),
    }
    desired, _missing = asyncio.run(
        sync.load_desired_rows(
            db, windows=_windows((1, -2, 2)), now=NOW, public=False, source_rows=source
        )
    )
    assert set(desired) == {"soma-task"}


def test_a_task_the_source_file_has_no_row_for_is_reported_missing():
    db = _FakeDb([("known", SOMA_BENCHMARK, 2), ("unknown", SOMA_BENCHMARK, 2)])
    desired, missing = asyncio.run(
        sync.load_desired_rows(
            db,
            windows=_windows((1, -2, 2)),
            now=NOW,
            public=False,
            source_rows={"known": _row("known")},
        )
    )
    assert set(desired) == {"known"}
    assert missing == {"unknown"}


# ── applying a tick ────────────────────────────────────────────────────────


class _FakeHub:
    """Stands in for hf_registry, recording what a tick would have written."""

    def __init__(self, *, published: bytes | None = None, private: bool = True):
        self.published = published
        self.private = private
        self.commits: list[tuple[str, int]] = []
        self.squashes = 0
        self.commit_depth = 1
        self.visibility_calls: list[bool] = []

    # metadata
    def repository_exists(self, repo):
        return True

    def is_private(self, repo):
        return self.private

    def set_private(self, repo, *, private):
        self.visibility_calls.append(private)
        self.private = private
        return self.private

    def commit_count(self, repo):
        return self.commit_depth

    # content
    def download_file(self, repo, path):
        return self.published

    def ensure_within_size_limit(self, path, content):
        return None

    def commit(self, repo, *, files, summary):
        content = files[PATH]
        self.published = content
        self.commits.append((summary, len(sync.parse_rows(content))))
        self.commit_depth += 1
        return "commit-sha"

    def super_squash(self, repo, *, summary):
        self.squashes += 1
        self.commit_depth = 1
        return "squash-sha"


@pytest.fixture
def hub(monkeypatch):
    fake = _FakeHub()
    monkeypatch.setattr(sync, "hf", fake)
    return fake


def _run_tick(db, *, windows, public, source, tmp_path, monkeypatch):
    source_file = tmp_path / "soma_tasks.jsonl"
    source_file.write_bytes(_jsonl(*source))
    monkeypatch.setattr(
        sync.settings, "hf_dataset_source_file", str(source_file), raising=False
    )
    return asyncio.run(
        sync.run_dataset_sync_tick(db=db, windows=windows, public=public, now=NOW)
    )


def test_private_tick_prunes_and_squashes(hub, tmp_path, monkeypatch):
    hub.published = _jsonl(_row("old"))
    db = _FakeDb([("wanted", SOMA_BENCHMARK, 2)])

    summary = _run_tick(
        db,
        windows=_windows((1, 1, 5)),
        public=False,
        source=[_row("wanted")],
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
    )

    assert summary["pruned"] is True
    assert summary["removed"] == ("old",)
    assert sorted(sync.parse_rows(hub.published)) == ["wanted"]
    assert hub.squashes == 1


def test_public_tick_never_removes_a_row(hub, tmp_path, monkeypatch):
    """A row a validator may be fetching must not disappear from under it."""
    hub.published = _jsonl(_row("in-flight"))
    hub.private = False
    db = _FakeDb([("wanted", SOMA_BENCHMARK, 2)])

    summary = _run_tick(
        db,
        windows=_windows((1, -1, 5)),
        public=True,
        source=[_row("wanted")],
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
    )

    assert summary["pruned"] is False
    assert summary["removed"] == ()
    assert sorted(sync.parse_rows(hub.published)) == ["in-flight", "wanted"]


def test_unchanged_rows_produce_no_commit(hub, tmp_path, monkeypatch):
    hub.published = sync.serialize_rows({"wanted": _row("wanted")})
    db = _FakeDb([("wanted", SOMA_BENCHMARK, 2)])

    summary = _run_tick(
        db,
        windows=_windows((1, 1, 5)),
        public=False,
        source=[_row("wanted")],
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
    )

    assert summary["unchanged"] is True
    assert hub.commits == []
    assert hub.squashes == 0


def test_a_missing_repository_aborts_the_tick(hub, tmp_path, monkeypatch):
    monkeypatch.setattr(hub, "repository_exists", lambda repo: False)
    db = _FakeDb([("wanted", SOMA_BENCHMARK, 2)])
    with pytest.raises(sync.TaskDatasetSyncError):
        _run_tick(
            db,
            windows=_windows((1, 1, 5)),
            public=False,
            source=[_row("wanted")],
            tmp_path=tmp_path,
            monkeypatch=monkeypatch,
        )


def test_tick_publishes_a_readiness_snapshot(hub, tmp_path, monkeypatch):
    db = _FakeDb([("wanted", SOMA_BENCHMARK, 2), ("unbuilt", SOMA_BENCHMARK, 2)])

    _run_tick(
        db,
        windows=_windows((1, 1, 5)),
        public=False,
        source=[_row("wanted")],
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
    )

    snapshot = sync.current_snapshot()
    assert snapshot is not None
    assert snapshot.ready_instance_ids == frozenset({"wanted"})
    assert snapshot.pending_instance_ids == frozenset({"unbuilt"})


# ── visibility ─────────────────────────────────────────────────────────────


def test_visibility_reports_no_change_when_already_correct(hub):
    hub.private = True
    assert sync.reconcile_visibility(public=False) == "private"
    assert hub.visibility_calls == []


def test_visibility_flips_to_public_and_back(hub):
    hub.private = True
    assert sync.reconcile_visibility(public=True) == "changed_to_public"
    assert sync.reconcile_visibility(public=False) == "changed_to_private"
    assert hub.visibility_calls == [False, True]


def test_a_request_that_did_not_move_the_repository_is_an_error(hub, monkeypatch):
    """Accepted is not the same claim as applied, and only the second one is safe."""
    hub.private = True
    monkeypatch.setattr(hub, "set_private", lambda repo, *, private: True)
    assert sync.reconcile_visibility(public=True).startswith("error:")


def test_publishing_a_deep_history_warns_but_still_publishes(hub, monkeypatch):
    """Holding the flip back over history hygiene would stall the whole competition."""
    warnings: list[str] = []
    monkeypatch.setattr(
        sync.logger, "warning", lambda event, **_kwargs: warnings.append(event)
    )
    hub.private = True
    hub.commit_depth = 7

    assert sync.reconcile_visibility(public=True) == "changed_to_public"
    assert warnings == ["hf_dataset_history_not_squashed"]


def test_a_single_commit_history_publishes_without_a_warning(hub, monkeypatch):
    warnings: list[str] = []
    monkeypatch.setattr(
        sync.logger, "warning", lambda event, **_kwargs: warnings.append(event)
    )
    hub.private = True
    hub.commit_depth = 1

    assert sync.reconcile_visibility(public=True) == "changed_to_public"
    assert warnings == []


# ── dispatch gate ──────────────────────────────────────────────────────────


def _block_reason(instance_id: str, *, benchmark_name: str = SOMA_BENCHMARK):
    return sync.dispatch_block_reason(
        benchmark_name=benchmark_name, instance_id=instance_id, screener_stage=2
    )


def test_gate_blocks_before_the_first_tick():
    assert _block_reason("anything") == sync.BLOCK_REASON_SYNC_PENDING


def test_gate_passes_a_published_row_and_blocks_an_unpublished_one():
    sync._publish_snapshot(
        sync.TaskDatasetSnapshot(
            repository=REPO,
            ready_instance_ids=frozenset({"ready"}),
            pending_instance_ids=frozenset({"pending"}),
            updated_at=NOW,
        )
    )
    assert _block_reason("ready") is None
    assert _block_reason("pending") == sync.BLOCK_REASON_ROW_MISSING


def test_gate_holds_back_a_published_row_while_the_dataset_is_private():
    """Rows are committed while the repository is still private, so publication alone
    does not mean a sandbox can read them."""
    sync.publish_visibility(False)
    sync._publish_snapshot(
        sync.TaskDatasetSnapshot(
            repository=REPO,
            ready_instance_ids=frozenset({"ready"}),
            pending_instance_ids=frozenset(),
            updated_at=NOW,
        )
    )

    assert _block_reason("ready") == sync.BLOCK_REASON_DATASET_PRIVATE


def test_gate_releases_the_row_once_the_dataset_is_observed_public():
    sync.publish_visibility(True)
    sync._publish_snapshot(
        sync.TaskDatasetSnapshot(
            repository=REPO,
            ready_instance_ids=frozenset({"ready"}),
            pending_instance_ids=frozenset(),
            updated_at=NOW,
        )
    )

    assert _block_reason("ready") is None


def test_reconcile_visibility_reports_what_it_observed(hub):
    """The gate is fed by the reconcile itself, so the two cannot disagree."""
    hub.private = True
    sync.reconcile_visibility(public=False)
    assert sync.observed_visibility() is False

    sync.reconcile_visibility(public=True)
    assert sync.observed_visibility() is True


def test_a_flip_that_did_not_apply_leaves_the_gate_closed(hub, monkeypatch):
    """Accepted-but-ignored must not read back as public, or runs would be released
    against a repository nobody can pull from."""
    hub.private = True
    monkeypatch.setattr(hub, "set_private", lambda repo, *, private: True)

    assert sync.reconcile_visibility(public=True).startswith("error:")
    assert sync.observed_visibility() is False


def test_gate_ignores_swebench_tasks():
    assert _block_reason("django__django-13821", benchmark_name=SWEBENCH_BENCHMARK) is None


def test_gate_is_open_while_the_loop_is_not_running():
    """An armed gate with nothing to feed it would hold every SOMA run forever."""
    sync.disarm_dispatch_gate()
    assert _block_reason("anything") is None


def test_gate_is_open_until_the_consumers_read_the_dataset(monkeypatch):
    """Gating on an unpublished row stops nothing while hosts read a local file."""
    monkeypatch.setattr(sync.settings, "hf_dataset_block_dispatch", False, raising=False)
    assert _block_reason("anything") is None


def test_gate_is_open_when_the_sync_is_disabled(monkeypatch):
    monkeypatch.setattr(sync.settings, "hf_dataset_enabled", False, raising=False)
    assert _block_reason("anything") is None


def test_visibility_task_arms_and_disarms_the_gate(monkeypatch):
    """The gate follows the loop's lifetime, not the setting's value."""
    from app.services import dockerhub_visibility as visibility

    sync.disarm_dispatch_gate()
    monkeypatch.setattr(visibility.settings, "dockerhub_visibility_enabled", True, raising=False)
    monkeypatch.setattr(visibility.settings, "dockerhub_task_repositories", [REPO], raising=False)

    class _App:
        class state:
            dockerhub_visibility_task = None

    async def _start_then_stop():
        visibility.start_dockerhub_visibility_task(_App())
        assert sync.dispatch_gate_armed() is True
        await visibility.stop_dockerhub_visibility_task(_App())

    asyncio.run(_start_then_stop())
    assert sync.dispatch_gate_armed() is False
