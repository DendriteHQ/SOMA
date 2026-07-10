from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

TESTS_DIR = os.path.dirname(__file__)
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if MCP_PLATFORM_DIR not in sys.path:
    sys.path.insert(0, MCP_PLATFORM_DIR)

os.environ["DEBUG"] = "false"
os.environ.setdefault("PRIVATE_NETWORK_CIDRS", "[]")
os.environ.setdefault("TRUSTED_PROXY_CIDRS", "[]")
os.environ.setdefault("SANDBOX_SERVICE_URL", "http://localhost")

from app.services import swebench_screening


class _ExecuteResult:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    def all(self) -> list[tuple[object, ...]]:
        return self._rows


@pytest.mark.asyncio
async def test_evaluate_screening_fails_when_task_pass_count_is_too_low(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)

    monkeypatch.setattr(
        swebench_screening.settings,
        "swebench_screening_pass_ratio",
        1.0,
        raising=False,
    )
    monkeypatch.setattr(
        swebench_screening.settings,
        "swebench_screening_min_passed_tasks",
        0,
        raising=False,
    )
    monkeypatch.setattr(
        swebench_screening.settings,
        "swebench_screening_min_weighted_token_saving_ratio",
        0.0,
        raising=False,
    )

    complete, passed = await swebench_screening.evaluate_screening_for_script(
        screener_task_ids=[101, 102],
        task_repeats={101: 1, 102: 1},
        baseline_weighted_by_task_attempt={
            (101, 1, "swebench_verified"): 100.0,
            (102, 1, "swebench_verified"): 100.0,
        },
        screening_by_task_attempt={
            (101, 1, "swebench_verified"): (True, now, 80.0),
            (102, 1, "swebench_verified"): (False, now, 80.0),
        },
    )

    assert (complete, passed) == (True, False)


@pytest.mark.asyncio
async def test_evaluate_screening_is_incomplete_when_run_or_baseline_is_missing() -> None:
    now = datetime.now(timezone.utc)

    missing_run = await swebench_screening.evaluate_screening_for_script(
        screener_task_ids=[101],
        task_repeats={101: 1},
        baseline_weighted_by_task_attempt={
            (101, 1, "swebench_verified"): 100.0,
        },
        screening_by_task_attempt={},
    )
    missing_baseline = await swebench_screening.evaluate_screening_for_script(
        screener_task_ids=[101],
        task_repeats={101: 1},
        baseline_weighted_by_task_attempt={},
        screening_by_task_attempt={
            (101, 1, "swebench_verified"): (True, now, 80.0),
        },
    )
    missing_score = await swebench_screening.evaluate_screening_for_script(
        screener_task_ids=[101],
        task_repeats={101: 1},
        baseline_weighted_by_task_attempt={
            (101, 1, "swebench_verified"): 100.0,
        },
        screening_by_task_attempt={
            (101, 1, "swebench_verified"): (True, None, 80.0),
        },
    )

    assert missing_run == (False, False)
    assert missing_baseline == (False, False)
    assert missing_score == (False, False)


@pytest.mark.asyncio
async def test_load_latest_scripts_for_competition_queries_latest_upload_per_miner() -> None:
    db = AsyncMock()
    db.execute = AsyncMock(
        return_value=_ExecuteResult(
            [
                (12, 1, "hk-pass"),
                (22, 2, "hk-latest-fails"),
            ]
        )
    )

    scripts = await swebench_screening.load_latest_scripts_for_competition(
        db,
        competition_id=7,
    )

    assert [(script.script_id, script.miner_fk, script.ss58) for script in scripts] == [
        (12, 1, "hk-pass"),
        (22, 2, "hk-latest-fails"),
    ]

    sql = str(db.execute.await_args_list[0].args[0])
    params = db.execute.await_args_list[0].args[1]
    assert "ROW_NUMBER() OVER" in sql
    assert "PARTITION BY s.miner_fk" in sql
    assert "u.competition_fk = :competition_id" in sql
    assert "m.miner_banned_status = FALSE" in sql
    assert "FROM miner_openrouter_api_keys mok" in sql
    assert "mok.revoked_at IS NULL" in sql
    assert params == {"competition_id": 7}


@pytest.mark.asyncio
async def test_load_screening_passed_hotkeys_uses_latest_script_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)
    db = AsyncMock()

    monkeypatch.setattr(
        swebench_screening,
        "load_screener_task_repeats",
        AsyncMock(return_value=([101, 102], {101: 1, 102: 1})),
    )
    monkeypatch.setattr(
        swebench_screening,
        "load_latest_scripts_for_competition",
        AsyncMock(
            return_value=[
                swebench_screening.ScriptRef(
                    script_id=12,
                    miner_fk=1,
                    ss58="hk-pass",
                ),
                swebench_screening.ScriptRef(
                    script_id=22,
                    miner_fk=2,
                    ss58="hk-latest-fails",
                ),
            ]
        ),
    )
    monkeypatch.setattr(
        swebench_screening,
        "load_screening_baseline_weighted_tokens",
        AsyncMock(
            return_value={
                (101, 1, "swebench_verified"): 100.0,
                (102, 1, "swebench_verified"): 100.0,
            }
        ),
    )
    monkeypatch.setattr(
        swebench_screening,
        "load_screening_miner_states_for_scripts",
        AsyncMock(
            return_value={
                (12, 1): {
                    (101, 1, "swebench_verified"): (True, now, 80.0),
                    (102, 1, "swebench_verified"): (True, now, 80.0),
                },
                (22, 2): {
                    (101, 1, "swebench_verified"): (False, now, 80.0),
                    (102, 1, "swebench_verified"): (True, now, 80.0),
                },
            }
        ),
    )
    monkeypatch.setattr(
        swebench_screening.settings,
        "swebench_screening_pass_ratio",
        1.0,
        raising=False,
    )
    monkeypatch.setattr(
        swebench_screening.settings,
        "swebench_screening_min_passed_tasks",
        0,
        raising=False,
    )
    monkeypatch.setattr(
        swebench_screening.settings,
        "swebench_screening_min_weighted_token_saving_ratio",
        0.1,
        raising=False,
    )

    hotkeys = await swebench_screening.load_screening_passed_hotkeys_for_competition(db, competition_id=1)

    assert hotkeys == ["hk-pass"]
