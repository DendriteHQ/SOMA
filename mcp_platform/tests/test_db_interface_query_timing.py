from __future__ import annotations

from datetime import datetime, timezone
import time
import warnings

import pytest
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.config import settings
from app.db.interfaces.query_registry import discover_db_query_interfaces
from soma_shared.db.models.batch_challenge import BatchChallenge
from soma_shared.db.models.miner import Miner
from soma_shared.db.models.miner_upload import MinerUpload
from soma_shared.db.models.question import Question
from soma_shared.db.models.script import Script
from soma_shared.db.models.validator import Validator


def _postgres_dsn_or_skip() -> str:
    dsn = settings.get_postgres_dsn()
    if not dsn:
        pytest.skip(
            "POSTGRES_DSN is not available. Configure POSTGRES_DSN or RDS_SECRET_ID."
        )
    return dsn


def _load_specs_or_fail():
    specs, missing = discover_db_query_interfaces()
    assert not missing, (
        "Async db-interface functions missing @db_query_interface registration:\n"
        + "\n".join(f"- {name}" for name in missing)
    )
    assert specs, "No db query interfaces discovered in app/db/interfaces."
    return specs


def test_db_interface_queries_are_registered():
    _load_specs_or_fail()


async def _resolve_any_miner_hotkey(db) -> str | None:
    return await db.scalar(
        select(Miner.ss58).where(Miner.ss58.is_not(None)).order_by(Miner.id.asc()).limit(1)
    )


async def _resolve_competition_miner_hotkey(
    db,
    *,
    competition_id: int,
) -> str | None:
    return await db.scalar(
        select(Miner.ss58)
        .join(Script, Script.miner_fk == Miner.id)
        .join(MinerUpload, MinerUpload.script_fk == Script.id)
        .where(MinerUpload.competition_fk == competition_id)
        .where(Miner.ss58.is_not(None))
        .order_by(MinerUpload.created_at.asc(), Miner.id.asc())
        .limit(1)
    )


async def _resolve_runtime_kwargs(
    db,
    *,
    qualified_name: str,
    kwargs: dict,
) -> dict:
    resolved = dict(kwargs)
    now = datetime.now(timezone.utc)
    competition_id_raw = resolved.get("competition_id")
    competition_id: int | None = None
    if competition_id_raw is not None:
        try:
            competition_id = int(competition_id_raw)
        except (TypeError, ValueError):
            competition_id = None

    for hotkey_field in ("miner_hotkey", "miner_ss58"):
        if hotkey_field not in resolved:
            continue
        hotkey = resolved.get(hotkey_field)
        if hotkey not in (None, "", "sample-hotkey"):
            continue

        if competition_id is not None:
            comp_hotkey = await _resolve_competition_miner_hotkey(
                db,
                competition_id=competition_id,
            )
            if comp_hotkey:
                resolved[hotkey_field] = comp_hotkey
                continue
            raise AssertionError(
                f"{qualified_name}: could not resolve {hotkey_field} for competition_id={competition_id}"
            )

        any_hotkey = await _resolve_any_miner_hotkey(db)
        if any_hotkey:
            resolved[hotkey_field] = any_hotkey
            continue
        raise AssertionError(
            f"{qualified_name}: could not resolve {hotkey_field}; no miners found in database"
        )

    needs_upsert_values = any(
        key in resolved and not resolved.get(key)
        for key in ("answer_values", "score_values", "rollup_values")
    )
    if needs_upsert_values:
        batch_challenge_id = await db.scalar(
            select(BatchChallenge.id).order_by(BatchChallenge.id.asc()).limit(1)
        )
        question_id = await db.scalar(
            select(Question.id).order_by(Question.id.asc()).limit(1)
        )
        validator_id = await db.scalar(
            select(Validator.id).order_by(Validator.id.asc()).limit(1)
        )
        if batch_challenge_id is None or question_id is None or validator_id is None:
            raise AssertionError(
                f"{qualified_name}: cannot build sample upsert values "
                "(need existing batch_challenge, question, validator rows)"
            )
        if "answer_values" in resolved and not resolved["answer_values"]:
            resolved["answer_values"] = [
                {
                    "batch_challenge_fk": int(batch_challenge_id),
                    "question_fk": int(question_id),
                    "produced_answer": "test-answer",
                    "uploaded_at": now,
                }
            ]
        if "score_values" in resolved and not resolved["score_values"]:
            resolved["score_values"] = [
                {
                    "batch_challenge_fk": int(batch_challenge_id),
                    "question_fk": int(question_id),
                    "validator_fk": int(validator_id),
                    "score": 0.0,
                    "details": {"source": "timing-test"},
                    "uploaded_at": now,
                }
            ]
        if "rollup_values" in resolved and not resolved["rollup_values"]:
            resolved["rollup_values"] = [
                {
                    "batch_challenge_fk": int(batch_challenge_id),
                    "validator_fk": int(validator_id),
                    "score": 0.0,
                    "created_at": now,
                }
            ]

    return resolved


@pytest.mark.asyncio
async def test_db_interface_query_timings_warn_if_slow(request: pytest.FixtureRequest):
    dsn = _postgres_dsn_or_skip()
    specs = _load_specs_or_fail()
    print_all_query_times = bool(request.config.getoption("--print-all-query-times"))

    engine = create_async_engine(dsn, pool_pre_ping=True)
    session_maker = async_sessionmaker(bind=engine, expire_on_commit=False)
    statement_counter = {"count": 0}
    timing_rows: list[tuple[str, float, int]] = []

    def _before_cursor_execute(*_args, **_kwargs):
        statement_counter["count"] += 1

    event.listen(
        engine.sync_engine,
        "before_cursor_execute",
        _before_cursor_execute,
    )

    try:
        for spec in specs:
            kwargs = spec.sample_kwargs()

            async with session_maker() as db:
                kwargs = await _resolve_runtime_kwargs(
                    db,
                    qualified_name=spec.qualified_name,
                    kwargs=kwargs,
                )
                before_sql_count = statement_counter["count"]
                started = time.perf_counter()
                try:
                    await spec.function(db=db, **kwargs)
                except Exception as exc:  # pragma: no cover - integration path
                    pytest.fail(
                        f"Query interface failed: {spec.qualified_name}({kwargs}) -> {exc!r}"
                    )
                finally:
                    if db.in_transaction():
                        await db.rollback()
                elapsed = time.perf_counter() - started
                executed_sql_count = statement_counter["count"] - before_sql_count

            if executed_sql_count <= 0:
                pytest.fail(
                    f"Query interface did not execute SQL: {spec.qualified_name}({kwargs})"
                )

            timing_rows.append((spec.qualified_name, elapsed, executed_sql_count))
            if elapsed > spec.threshold_seconds:
                warnings.warn(
                    (
                        f"Slow DB interface query: {spec.qualified_name} took "
                        f"{elapsed:.3f}s (> {spec.threshold_seconds:.3f}s)"
                    ),
                    UserWarning,
                    stacklevel=1,
                )
        if print_all_query_times:
            request.config._db_interface_query_timings = list(timing_rows)
    finally:
        event.remove(
            engine.sync_engine,
            "before_cursor_execute",
            _before_cursor_execute,
        )
        await engine.dispose()
