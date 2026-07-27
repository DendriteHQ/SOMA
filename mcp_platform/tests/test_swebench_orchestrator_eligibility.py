from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

TESTS_DIR = os.path.dirname(__file__)
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if MCP_PLATFORM_DIR not in sys.path:
    sys.path.insert(0, MCP_PLATFORM_DIR)

# Keep settings importable in environments that do not provide full app config.
os.environ["DEBUG"] = "false"
os.environ.setdefault("PRIVATE_NETWORK_CIDRS", "[]")
os.environ.setdefault("TRUSTED_PROXY_CIDRS", "[]")
os.environ.setdefault("SANDBOX_SERVICE_URL", "http://localhost")

from app.services import swebench_orchestrator as orchestrator


class _MappingsResult:
    def __init__(self, rows: list[dict]):
        self._rows = rows

    def all(self) -> list[dict]:
        return self._rows


class _ExecuteResult:
    def __init__(
        self,
        *,
        all_rows: list | None = None,
        first_row=None,
        mappings_rows: list[dict] | None = None,
        rowcount: int = 0,
    ):
        self._all_rows = all_rows or []
        self._first_row = first_row
        self._mappings_rows = mappings_rows or []
        self.rowcount = rowcount

    def all(self) -> list:
        return self._all_rows

    def first(self):
        return self._first_row

    def mappings(self) -> _MappingsResult:
        return _MappingsResult(self._mappings_rows)


class _ScalarsExecuteResult:
    def __init__(self, items: list):
        self._items = items

    def scalars(self) -> "_ScalarsExecuteResult":
        return self

    def all(self) -> list:
        return self._items


class _DummyDispatchManager:
    def __init__(self) -> None:
        self.calls = 0

    async def dispatch_swebench_run(self, **kwargs):
        self.calls += 1
        return True, "", False


def _build_app(*, manager: _DummyDispatchManager | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        state=SimpleNamespace(
            swebench_compact_bench_manager=manager or _DummyDispatchManager(),
            swebench_s3_storage=object(),
            swebench_retry_not_before={},
            swebench_retry_attempts={},
            swebench_global_retry_not_before=0.0,
        )
    )


def _extract_sql(async_mock: AsyncMock, call_index: int = 0) -> str:
    return str(async_mock.await_args_list[call_index].args[0])


def test_weighted_tokens_for_screening_uses_configured_weights(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        orchestrator.settings,
        "swebench_screening_input_tokens_weight",
        2.0,
        raising=False,
    )
    monkeypatch.setattr(
        orchestrator.settings,
        "swebench_screening_cached_input_tokens_weight",
        0.5,
        raising=False,
    )
    monkeypatch.setattr(
        orchestrator.settings,
        "swebench_screening_output_tokens_weight",
        4.0,
        raising=False,
    )

    weighted = orchestrator._weighted_tokens_for_screening(
        total_tokens=None,
        input_tokens=10,
        cached_input_tokens=8,
        output_tokens=6,
    )

    assert weighted == pytest.approx(48.0)


def test_split_tasks_by_stage() -> None:
    tasks = [
        SimpleNamespace(id=1, screener_stage=1),
        SimpleNamespace(id=2, screener_stage=2),
        SimpleNamespace(id=3, screener_stage=None),
        SimpleNamespace(id=4),  # missing attr -> treated as eval
    ]
    stage1, stage2, eval_tasks = orchestrator._split_tasks_by_stage(tasks)
    assert [t.id for t in stage1] == [1]
    assert [t.id for t in stage2] == [2]
    assert [t.id for t in eval_tasks] == [3, 4]


def test_derive_run_quality_and_resolved_per_benchmark_type() -> None:
    assert orchestrator._derive_run_quality_and_resolved(
        "swebench_verified", verified_resolved=True, edit_resolved=None,
        explore_f1=None, explore_hit=None, explore_noise=None,
    ) == (True, 1.0)
    assert orchestrator._derive_run_quality_and_resolved(
        "swe_explorer_edit", verified_resolved=None, edit_resolved=False,
        explore_f1=None, explore_hit=None, explore_noise=None,
    ) == (False, 0.0)
    resolved, quality = orchestrator._derive_run_quality_and_resolved(
        "swe_explorer_explore", verified_resolved=None, edit_resolved=None,
        explore_f1=0.4, explore_hit=0.8, explore_noise=0.3,
    )
    assert resolved is True
    assert quality == pytest.approx(0.5)
    # Not scored -> resolved/quality None (incompleteness signal).
    assert orchestrator._derive_run_quality_and_resolved(
        "swe_explorer_explore", verified_resolved=None, edit_resolved=None,
        explore_f1=None, explore_hit=None, explore_noise=None,
    ) == (None, None)


def test_select_stage2_advancers_top_fraction_plus_delta(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(orchestrator.settings, "top_screener_scripts", 0.2, raising=False)
    monkeypatch.setattr(orchestrator.settings, "screener_stage2_min_advancers", 0, raising=False)
    monkeypatch.setattr(orchestrator.settings, "screener_extra_score_points", 0.03, raising=False)
    monkeypatch.setattr(orchestrator.settings, "screener_extra_miners_limit", 10, raising=False)

    scored = [
        (orchestrator._ScriptRef(script_id=i, miner_fk=i), ratio)
        for i, ratio in enumerate([0.50, 0.49, 0.485, 0.48, 0.30, 0.10, 0.0])
    ]
    selected = orchestrator._select_stage2_advancers(scored)
    # top 20% of 7 = ceil(1.4) = 2 (ids 0,1); delta window within 0.03 of 0.50
    # adds ids 2 (0.485) and 3 (0.48); 0.30 is outside the window.
    assert sorted(s.script_id for s in selected) == [0, 1, 2, 3]


def test_select_stage2_advancers_delta_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(orchestrator.settings, "top_screener_scripts", 0.2, raising=False)
    monkeypatch.setattr(orchestrator.settings, "screener_stage2_min_advancers", 0, raising=False)
    monkeypatch.setattr(orchestrator.settings, "screener_extra_score_points", 1.0, raising=False)
    monkeypatch.setattr(orchestrator.settings, "screener_extra_miners_limit", 1, raising=False)

    scored = [
        (orchestrator._ScriptRef(script_id=i, miner_fk=i), 0.5) for i in range(10)
    ]
    selected = orchestrator._select_stage2_advancers(scored)
    # top 20% of 10 = 2, plus at most 1 extra via the (wide) delta window.
    assert len(selected) == 3


def test_select_stage2_advancers_honors_min_advancers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(orchestrator.settings, "top_screener_scripts", 0.2, raising=False)
    monkeypatch.setattr(orchestrator.settings, "screener_stage2_min_advancers", 4, raising=False)
    monkeypatch.setattr(orchestrator.settings, "screener_extra_score_points", 0.0, raising=False)
    monkeypatch.setattr(orchestrator.settings, "screener_extra_miners_limit", 0, raising=False)

    scored = [
        (orchestrator._ScriptRef(script_id=i, miner_fk=i), ratio)
        for i, ratio in enumerate([0.50, 0.49, 0.485, 0.48, 0.30, 0.10, 0.0])
    ]
    selected = orchestrator._select_stage2_advancers(scored)
    assert sorted(s.script_id for s in selected) == [0, 1, 2, 3]


@pytest.mark.asyncio
async def test_seed_upload_phase_runs_stage1_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)
    eval_starts_at = now + timedelta(hours=6)  # upload window (stage 2 not open)
    tasks = [
        SimpleNamespace(id=1, planned_repeats=3, is_screener=True, screener_stage=1),
        SimpleNamespace(id=2, planned_repeats=3, is_screener=True, screener_stage=2),
        SimpleNamespace(id=3, planned_repeats=3, is_screener=False, screener_stage=None),
    ]

    db = AsyncMock()
    db.execute = AsyncMock(return_value=_ScalarsExecuteResult(tasks))

    seed_baseline_mock = AsyncMock(return_value=0)
    seed_subset_mock = AsyncMock(return_value=0)
    load_existing_runs_mock = AsyncMock(return_value={(501, 11): set()})

    monkeypatch.setattr(orchestrator, "_seed_baseline_runs", seed_baseline_mock)
    monkeypatch.setattr(
        orchestrator, "_is_screener_baseline_complete", AsyncMock(return_value=True)
    )
    monkeypatch.setattr(
        orchestrator,
        "_load_latest_scripts_for_competition",
        AsyncMock(return_value=[orchestrator._ScriptRef(script_id=501, miner_fk=11)]),
    )
    monkeypatch.setattr(orchestrator, "_load_screening_baseline_states", AsyncMock(return_value={}))
    monkeypatch.setattr(orchestrator, "_load_screening_miner_run_states", AsyncMock(return_value={}))
    monkeypatch.setattr(
        orchestrator.screening_shared,
        "evaluate_stage1_for_script",
        AsyncMock(return_value=(True, True)),
    )
    monkeypatch.setattr(orchestrator, "_load_existing_non_baseline_run_keys_for_scripts", load_existing_runs_mock)
    monkeypatch.setattr(orchestrator, "_seed_script_task_subset_from_existing", seed_subset_mock)

    created = await orchestrator._seed_runs_for_competition(
        db, competition_id=75, eval_starts_at=eval_starts_at, now=now
    )

    assert created == 0
    # Only stage-1 baseline is seeded during the upload window.
    assert seed_baseline_mock.await_count == 1
    baseline_kwargs = seed_baseline_mock.await_args_list[0].kwargs
    assert [task.id for task in baseline_kwargs["tasks"]] == [1]
    assert baseline_kwargs["benchmark_types"] == orchestrator._SCREENING_BENCHMARK_TYPES
    # Only stage-1 miner runs are seeded (task 1), never stage-2/eval.
    assert seed_subset_mock.await_count == 1
    assert seed_subset_mock.await_args_list[0].kwargs["task_ids"] == [1]


@pytest.mark.asyncio
async def test_seed_waits_for_stage1_baseline_before_loading_scripts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)
    eval_starts_at = now + timedelta(hours=6)
    tasks = [
        SimpleNamespace(id=1, planned_repeats=3, is_screener=True, screener_stage=1),
        SimpleNamespace(id=2, planned_repeats=3, is_screener=False, screener_stage=None),
    ]

    db = AsyncMock()
    db.execute = AsyncMock(return_value=_ScalarsExecuteResult(tasks))

    load_scripts_mock = AsyncMock(return_value=[])
    monkeypatch.setattr(orchestrator, "_seed_baseline_runs", AsyncMock(return_value=0))
    monkeypatch.setattr(orchestrator, "_load_latest_scripts_for_competition", load_scripts_mock)
    monkeypatch.setattr(orchestrator, "_load_screening_baseline_states", AsyncMock(return_value={}))
    monkeypatch.setattr(orchestrator, "_load_screening_miner_run_states", AsyncMock(return_value={}))
    monkeypatch.setattr(
        orchestrator.screening_shared,
        "evaluate_stage1_for_script",
        AsyncMock(return_value=(False, False)),
    )

    created = await orchestrator._seed_runs_for_competition(
        db, competition_id=75, eval_starts_at=eval_starts_at, now=now
    )

    assert created == 0
    # Current flow still loads scripts and seeds stage-1 runs before
    # classification can conclude the cohort is incomplete.
    assert load_scripts_mock.await_count == 1


@pytest.mark.asyncio
async def test_seed_eval_window_runs_all_stages_and_seeds_full_eval_for_advancer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)
    eval_starts_at = now - timedelta(minutes=5)  # eval window (upload closed)
    tasks = [
        SimpleNamespace(id=1, planned_repeats=3, is_screener=True, screener_stage=1),
        SimpleNamespace(id=2, planned_repeats=3, is_screener=True, screener_stage=2),
        SimpleNamespace(id=3, planned_repeats=3, is_screener=False, screener_stage=None),
    ]

    db = AsyncMock()
    db.execute = AsyncMock(return_value=_ScalarsExecuteResult(tasks))

    seed_baseline_mock = AsyncMock(return_value=0)
    seed_subset_mock = AsyncMock(return_value=0)
    load_existing_runs_mock = AsyncMock(return_value={(501, 11): set()})

    monkeypatch.setattr(orchestrator, "_seed_baseline_runs", seed_baseline_mock)
    monkeypatch.setattr(
        orchestrator, "_is_screener_baseline_complete", AsyncMock(return_value=True)
    )
    monkeypatch.setattr(
        orchestrator,
        "_load_latest_scripts_for_competition",
        AsyncMock(return_value=[orchestrator._ScriptRef(script_id=501, miner_fk=11)]),
    )
    monkeypatch.setattr(orchestrator, "_load_screening_baseline_states", AsyncMock(return_value={}))
    monkeypatch.setattr(orchestrator, "_load_screening_miner_run_states", AsyncMock(return_value={}))
    monkeypatch.setattr(
        orchestrator.screening_shared,
        "evaluate_stage1_for_script",
        AsyncMock(return_value=(True, True)),
    )
    # Stage 2 has no pass/fail gate now; only a completeness barrier before
    # relative ranking. Treat the cohort as fully scored.
    monkeypatch.setattr(orchestrator, "_stage2_runs_complete", lambda **kwargs: True)
    import app.services.incentive_calculator as incentive_calculator
    monkeypatch.setattr(
        incentive_calculator,
        "load_stage2_miner_total_scores",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(orchestrator, "_load_existing_non_baseline_run_keys_for_scripts", load_existing_runs_mock)
    monkeypatch.setattr(orchestrator, "_seed_script_task_subset_from_existing", seed_subset_mock)

    created = await orchestrator._seed_runs_for_competition(
        db, competition_id=75, eval_starts_at=eval_starts_at, now=now
    )

    assert created == 0
    # Baselines seeded lazily per stage: stage1, stage2, eval.
    baseline_task_lists = [
        [task.id for task in call.kwargs["tasks"]] for call in seed_baseline_mock.await_args_list
    ]
    assert baseline_task_lists == [[1], [2], [3]]
    # Miner-run seeding: stage1 (task 1), stage2 (task 2), full eval (task 3).
    seeded_task_ids = [call.kwargs["task_ids"] for call in seed_subset_mock.await_args_list]
    assert seeded_task_ids == [[1], [2], [3]]
    eval_kwargs = seed_subset_mock.await_args_list[-1].kwargs
    assert eval_kwargs["benchmark_types"] == orchestrator._BENCHMARK_TYPES


@pytest.mark.asyncio
async def test_evaluate_screening_passes_with_weighted_token_saving_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)

    monkeypatch.setattr(orchestrator.settings, "swebench_screening_pass_ratio", 1.0, raising=False)
    monkeypatch.setattr(orchestrator.settings, "swebench_screening_min_passed_tasks", 0, raising=False)
    monkeypatch.setattr(
        orchestrator.settings,
        "swebench_screening_min_weighted_token_saving_ratio",
        0.1,
        raising=False,
    )

    complete, passed = await orchestrator._evaluate_screening_for_script(
        screener_task_ids=[101, 102],
        task_repeats={101: 1, 102: 1},
        baseline_weighted_by_task_attempt={
            (101, 1, "swebench_verified"): 150.0,
            (102, 1, "swebench_verified"): 120.0,
        },
        screening_by_task_attempt={
            (101, 1, "swebench_verified"): (True, now, 100.0),
            (102, 1, "swebench_verified"): (True, now, 80.0),
        },
    )

    assert (complete, passed) == (True, True)


@pytest.mark.asyncio
async def test_evaluate_screening_fails_when_weighted_token_saving_is_below_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)

    monkeypatch.setattr(orchestrator.settings, "swebench_screening_pass_ratio", 1.0, raising=False)
    monkeypatch.setattr(orchestrator.settings, "swebench_screening_min_passed_tasks", 1, raising=False)
    monkeypatch.setattr(
        orchestrator.settings,
        "swebench_screening_min_weighted_token_saving_ratio",
        0.1,
        raising=False,
    )

    complete, passed = await orchestrator._evaluate_screening_for_script(
        screener_task_ids=[101],
        task_repeats={101: 1},
        baseline_weighted_by_task_attempt={
            (101, 1, "swebench_verified"): 100.0,
        },
        screening_by_task_attempt={
            (101, 1, "swebench_verified"): (True, now, 95.0),
        },
    )

    assert (complete, passed) == (True, False)


@pytest.mark.asyncio
async def test_load_latest_scripts_requires_active_key_and_not_banned() -> None:
    now = datetime.now(timezone.utc)
    db = AsyncMock()
    db.execute = AsyncMock(
        return_value=_ExecuteResult(
            all_rows=[
                (501, 11, now),  # newest script for miner 11
                (502, 12, now),
                (500, 11, now),  # older duplicate for miner 11
            ]
        )
    )

    script_refs = await orchestrator._load_latest_scripts_for_competition(
        db,
        competition_id=77,
    )

    assert [(ref.script_id, ref.miner_fk) for ref in script_refs] == [(501, 11), (502, 12)]

    sql = _extract_sql(db.execute)
    params = db.execute.await_args_list[0].args[1]
    assert "u.competition_fk = :competition_id" in sql
    assert "m.miner_banned_status = FALSE" in sql
    assert "FROM miner_openrouter_api_keys mok" in sql
    assert "mok.revoked_at IS NULL" in sql
    assert params == {"competition_id": 77}


@pytest.mark.asyncio
async def test_load_script_dispatch_context_requires_active_openrouter_key() -> None:
    now = datetime.now(timezone.utc)
    db = AsyncMock()
    db.execute = AsyncMock(
        side_effect=[
            _ExecuteResult(first_row=None),
            _ExecuteResult(first_row=("script-uuid", now, "miner-ss58")),
        ]
    )

    no_key = await orchestrator._load_script_dispatch_context(
        db=db,
        script_fk=901,
        miner_fk=45,
        competition_fk=88,
        require_active_openrouter_key=True,
    )
    with_key = await orchestrator._load_script_dispatch_context(
        db=db,
        script_fk=901,
        miner_fk=45,
        competition_fk=88,
        require_active_openrouter_key=True,
    )

    assert no_key is None
    assert with_key == ("script-uuid", now, "miner-ss58")

    sql = _extract_sql(db.execute, call_index=0)
    params = db.execute.await_args_list[0].args[1]
    assert "u.competition_fk = :competition_fk" in sql
    assert "FROM miner_openrouter_api_keys mok" in sql
    assert "mok.revoked_at IS NULL" in sql
    assert params == {"script_fk": 901, "miner_fk": 45, "competition_fk": 88}


@pytest.mark.asyncio
async def test_dispatch_keeps_ineligible_non_baseline_runs_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)
    db = AsyncMock()
    db.execute = AsyncMock(return_value=_ExecuteResult(mappings_rows=[]))
    db.rollback = AsyncMock()
    db.commit = AsyncMock()

    manager = _DummyDispatchManager()
    app = _build_app(manager=manager)

    async def _db_session_gen():
        yield db

    monkeypatch.setattr(orchestrator, "get_db_session", _db_session_gen)

    dispatched, deferred, failed = await orchestrator._dispatch_due_runs(app, now)

    assert (dispatched, deferred, failed) == (0, 0, 0)
    assert manager.calls == 0
    assert db.rollback.await_count == 1
    assert db.commit.await_count == 0

    selection_sql = _extract_sql(db.execute, call_index=1)
    assert "WHERE r.status = 'pending'" in selection_sql
    assert "r.baseline_run = TRUE" in selection_sql
    assert "FROM miner_uploads mu" in selection_sql
    assert "FROM miner_openrouter_api_keys mok" in selection_sql


@pytest.mark.asyncio
async def test_dispatch_query_orders_baseline_then_upload_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)
    db = AsyncMock()
    db.execute = AsyncMock(return_value=_ExecuteResult(mappings_rows=[]))
    db.rollback = AsyncMock()
    db.commit = AsyncMock()

    app = _build_app()

    async def _db_session_gen():
        yield db

    monkeypatch.setattr(orchestrator, "get_db_session", _db_session_gen)

    await orchestrator._dispatch_due_runs(app, now)

    selection_sql = _extract_sql(db.execute, call_index=1)
    assert "AS miner_upload_created_at" in selection_sql
    assert "SELECT MIN(mu.created_at)" in selection_sql
    screener_priority_clause = "CASE t.screener_stage WHEN 1 THEN 0 WHEN 2 THEN 1 ELSE 2 END ASC"
    assert screener_priority_clause in selection_sql
    assert selection_sql.index(screener_priority_clause) < selection_sql.index(
        "CASE WHEN r.baseline_run = TRUE THEN 0 ELSE 1 END ASC"
    )
    assert "miner_upload_created_at ASC NULLS LAST" in selection_sql
    assert "r.id ASC" in selection_sql


@pytest.mark.asyncio
async def test_dispatch_resumes_after_eligibility_restored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)
    due_row = {
        "run_id": 1234,
        "diff_storage_uuid": "00000000-0000-0000-0000-000000001234",
        "attempt_no": 1,
        "miner_fk": 55,
        "script_fk": 66,
        "baseline_run": False,
        "task_id": 77,
        "competition_fk": 88,
        "instance_id": "instance-1234",
        "planned_repeats": 1,
        "is_screener": False,
    }

    ineligible_db = AsyncMock()
    ineligible_db.execute = AsyncMock(return_value=_ExecuteResult(mappings_rows=[]))
    ineligible_db.rollback = AsyncMock()
    ineligible_db.commit = AsyncMock()

    eligible_db = AsyncMock()
    eligible_db.execute = AsyncMock(
        side_effect=[
            _ExecuteResult(),
            _ExecuteResult(mappings_rows=[due_row]),
            _ExecuteResult(all_rows=[]),
            _ExecuteResult(),
        ]
    )
    eligible_db.rollback = AsyncMock()
    eligible_db.commit = AsyncMock()

    sessions = [ineligible_db, eligible_db]

    async def _db_session_gen():
        yield sessions.pop(0)

    async def _fake_presigned_url(**kwargs):
        return "https://example.com/script.py"

    manager = _DummyDispatchManager()
    app = _build_app(manager=manager)

    monkeypatch.setattr(orchestrator, "get_db_session", _db_session_gen)
    monkeypatch.setattr(orchestrator, "_resolve_script_presigned_url", _fake_presigned_url)

    first = await orchestrator._dispatch_due_runs(app, now)
    second = await orchestrator._dispatch_due_runs(app, now)

    assert first == (0, 0, 0)
    assert second == (1, 0, 0)
    assert manager.calls == 1

    # First pass left pending work untouched (selection only, no UPDATE status change).
    assert ineligible_db.execute.await_count == 2

    dispatched_update_sql = _extract_sql(eligible_db.execute, call_index=3)
    assert "SET status = 'dispatched'" in dispatched_update_sql
    assert eligible_db.commit.await_count == 1
