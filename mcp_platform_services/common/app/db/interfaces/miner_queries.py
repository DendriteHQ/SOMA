from __future__ import annotations

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.interfaces.query_registry import db_query_interface
from soma_shared.db.models.competition import Competition
from soma_shared.db.models.competition_config import CompetitionConfig
from soma_shared.db.models.competition_timeframe import CompetitionTimeframe
from soma_shared.db.models.miner import Miner
from soma_shared.db.models.miner_upload import MinerUpload
from soma_shared.db.models.script import Script
from soma_shared.db.models.swe_bench_run import SweBenchRun
from soma_shared.db.models.swe_bench_task import SweBenchTask


@db_query_interface(sample_kwargs={"miner_hotkey": "sample-hotkey"})
async def get_miner_banned_status_by_hotkey(
    db: AsyncSession,
    *,
    miner_hotkey: str,
) -> bool:
    banned = await db.scalar(
        select(Miner.miner_banned_status).where(Miner.ss58 == miner_hotkey).limit(1)
    )
    return bool(banned)


@db_query_interface(sample_kwargs={"miner_hotkey": "sample-hotkey"})
async def get_latest_miner_upload_created_at(
    db: AsyncSession,
    *,
    miner_hotkey: str,
):
    result = await db.execute(
        select(MinerUpload.created_at)
        .join(Script, Script.id == MinerUpload.script_fk)
        .join(Miner, Miner.id == Script.miner_fk)
        .where(Miner.ss58 == miner_hotkey)
        .order_by(MinerUpload.created_at.desc())
        .limit(1)
    )
    return result.scalars().first()


@db_query_interface(sample_kwargs={"competition_id": 40, "miner_hotkey": "sample-hotkey"})
async def get_miner_upload_id_for_competition(
    db: AsyncSession,
    *,
    miner_hotkey: str,
    competition_id: int,
) -> int | None:
    existing_upload = await db.scalar(
        select(MinerUpload.id)
        .join(Script, Script.id == MinerUpload.script_fk)
        .join(Miner, Miner.id == Script.miner_fk)
        .where(Miner.ss58 == miner_hotkey)
        .where(MinerUpload.competition_fk == competition_id)
        .limit(1)
    )
    return int(existing_upload) if existing_upload is not None else None


@db_query_interface(sample_kwargs={})
async def get_latest_active_competition_timeframe(
    db: AsyncSession,
) -> CompetitionTimeframe | None:
    return await db.scalar(
        select(CompetitionTimeframe)
        .join(
            CompetitionConfig,
            CompetitionConfig.id == CompetitionTimeframe.competition_config_fk,
        )
        .join(
            Competition,
            Competition.id == CompetitionConfig.competition_fk,
        )
        .where(CompetitionConfig.is_active.is_(True))
        .order_by(Competition.created_at.desc(), CompetitionTimeframe.created_at.desc())
        .limit(1)
    )


@db_query_interface(sample_kwargs={})
async def get_latest_active_competition_and_timeframe(
    db: AsyncSession,
):
    result = await db.execute(
        select(Competition.id, CompetitionTimeframe)
        .join(
            CompetitionConfig,
            CompetitionConfig.id == CompetitionTimeframe.competition_config_fk,
        )
        .join(
            Competition,
            Competition.id == CompetitionConfig.competition_fk,
        )
        .where(CompetitionConfig.is_active.is_(True))
        .order_by(Competition.created_at.desc(), CompetitionTimeframe.created_at.desc())
        .limit(1)
    )
    return result.first()


@db_query_interface(sample_kwargs={"competition_id": 40, "miner_hotkey": "sample-hotkey"})
async def acquire_miner_upload_advisory_lock(
    db: AsyncSession,
    *,
    miner_hotkey: str,
    competition_id: int,
) -> None:
    bind = db.get_bind()
    if bind.dialect.name != "postgresql":
        return
    lock_key = f"miner-upload:{competition_id}:{miner_hotkey}"
    await db.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
        {"lock_key": lock_key},
    )


@db_query_interface(
    sample_kwargs={"miner_hotkey": "sample-hotkey", "instance_id": "sample-instance", "attempt_no": 1}
)
async def get_latest_miner_run_for_task_attempt(
    db: AsyncSession,
    *,
    miner_hotkey: str,
    instance_id: str,
    attempt_no: int,
    competition_name: str | None = None,
):
    """Latest non-baseline run of the miner for (instance_id, attempt_no).

    The same instance can be benchmarked in several competitions: ``competition_name``
    (matched ignoring case and surrounding whitespace) picks one, otherwise the newest
    run wins.
    """
    query = (
        select(
            SweBenchRun.id.label("run_id"),
            SweBenchRun.status,
            SweBenchRun.compression_logs_uuid,
            SweBenchTask.competition_fk,
            Competition.competition_name,
        )
        .join(SweBenchTask, SweBenchTask.id == SweBenchRun.task_fk)
        .join(Competition, Competition.id == SweBenchTask.competition_fk)
        .join(Miner, Miner.id == SweBenchRun.miner_fk)
        .where(Miner.ss58 == miner_hotkey)
        .where(SweBenchTask.instance_id == instance_id)
        .where(SweBenchRun.attempt_no == attempt_no)
        .where(SweBenchRun.baseline_run.is_(False))
    )
    if competition_name is not None:
        # Same expression as uq_competitions_competition_name_normalized, so it uses the index.
        query = query.where(
            func.lower(func.btrim(Competition.competition_name)) == competition_name.strip().lower()
        )
    result = await db.execute(
        query.order_by(SweBenchRun.created_at.desc(), SweBenchRun.id.desc()).limit(1)
    )
    return result.first()
