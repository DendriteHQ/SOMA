from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from soma_shared.db.models.competition import Competition
from soma_shared.db.models.competition_config import CompetitionConfig
from soma_shared.db.models.competition_timeframe import CompetitionTimeframe
from soma_shared.db.models.miner import Miner
from soma_shared.db.models.miner_upload import MinerUpload
from soma_shared.db.models.request import Request
from soma_shared.db.models.script import Script
from app.services.blob.script_storage import ScriptStorage

logger = logging.getLogger(__name__)


async def _select_active_competition_id(session: AsyncSession) -> int | None:
    now = datetime.now(timezone.utc)
    result = await session.execute(
        select(Competition.id)
        .join(
            CompetitionConfig,
            CompetitionConfig.competition_fk == Competition.id,
        )
        .join(
            CompetitionTimeframe,
            CompetitionTimeframe.competition_config_fk == CompetitionConfig.id,
        )
        .where(CompetitionConfig.is_active.is_(True))
        .where(CompetitionTimeframe.upload_starts_at <= now)
        .where(CompetitionTimeframe.upload_ends_at >= now)
        .order_by(Competition.created_at.desc())
        .limit(1)
    )
    competition_id = result.scalar_one_or_none()
    if competition_id is not None:
        return competition_id
    result = await session.execute(
        select(Competition.id)
        .join(
            CompetitionConfig,
            CompetitionConfig.competition_fk == Competition.id,
        )
        .where(CompetitionConfig.is_active.is_(True))
        .order_by(Competition.created_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def store_hot_script(
    session: AsyncSession,
    storage: ScriptStorage,
    *,
    miner_ss58: str,
    script: str,
    request_id: str,
    script_uuid: str | None = None,
    competition_id: int | None = None,
) -> Script:
    if not script_uuid:
        script_uuid = str(uuid.uuid4())

    date_prefix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = await storage.put_hot_script(
        miner_ss58,
        script_uuid,
        script,
        date_prefix=date_prefix,
    )
    request_fk = None
    if request_id:
        result = await session.execute(
            select(Request).where(Request.external_request_id == request_id)
        )
        request_row = result.scalars().first()
        if request_row is not None:
            request_fk = request_row.id
    result = await session.execute(select(Miner).where(Miner.ss58 == miner_ss58))
    miner = result.scalars().first()
    if miner is None:
        miner = Miner(ss58=miner_ss58)
        session.add(miner)
        await session.flush()

    if competition_id is None:
        competition_id = await _select_active_competition_id(session)
    if competition_id is None:
        raise LookupError("No active competition available for miner upload")

    script_row = Script(
        script_uuid=script_uuid,
        miner_fk=miner.id,
        request_fk=request_fk,
    )
    try:
        session.add(script_row)
        await session.flush()

        # Get active competition
        if competition_id is None:
            comp_result = await session.execute(
                select(Competition.id)
                .join(
                    CompetitionConfig,
                    CompetitionConfig.competition_fk == Competition.id,
                )
                .where(CompetitionConfig.is_active.is_(True))
                .limit(1)
            )
            competition_id = comp_result.scalar()

        session.add(
            MinerUpload(
                script_fk=script_row.id,
                request_fk=request_fk,
                competition_fk=competition_id,
            )
        )
        await session.commit()
        await session.refresh(script_row)
    except SQLAlchemyError:
        await session.rollback()
        try:
            await storage.delete(key)
        except Exception:
            logger.exception("script_upload_cleanup_failed", extra={"key": key})
        logger.exception("script_write_failed", extra={"key": key})
        raise

    return script_row
