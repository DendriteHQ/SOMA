from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
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
from app.core.logging import get_logger

logger = get_logger(__name__)


class DuplicateMinerUploadError(RuntimeError):
    pass


class BannedMinerUploadError(RuntimeError):
    pass


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
    write_succeeded = False
    script_row: Script | None = None

    try:
        request_fk = None
        if request_id:
            result = await session.execute(
                select(Request).where(Request.external_request_id == request_id)
            )
            request_row = result.scalars().first()
            if request_row is not None:
                request_fk = request_row.id

        insert_stmt = (
            pg_insert(Miner)
            .values(ss58=miner_ss58)
            .on_conflict_do_nothing(index_elements=[Miner.ss58])
            .returning(Miner.id)
        )
        insert_result = await session.execute(insert_stmt)
        miner_id = insert_result.scalar_one_or_none()
        if miner_id is None:
            result = await session.execute(
                select(Miner.id).where(Miner.ss58 == miner_ss58)
            )
            miner_id = result.scalar_one_or_none()
        if miner_id is None:
            raise LookupError(f"Failed to resolve miner for ss58={miner_ss58}")

        if competition_id is None:
            competition_id = await _select_active_competition_id(session)
        if competition_id is None:
            raise LookupError("No active competition available for miner upload")

        miner_row = await session.scalar(
            select(Miner).where(Miner.id == miner_id).with_for_update()
        )
        if miner_row is None:
            raise LookupError(f"Failed to resolve miner row for id={miner_id}")
        if miner_row.miner_banned_status:
            raise BannedMinerUploadError(
                "Miner is banned and cannot upload scripts"
            )

        existing_upload = await session.scalar(
            select(MinerUpload.id)
            .join(Script, Script.id == MinerUpload.script_fk)
            .where(Script.miner_fk == miner_id)
            .where(MinerUpload.competition_fk == competition_id)
            .limit(1)
        )
        if existing_upload is not None:
            raise DuplicateMinerUploadError(
                "Miner already uploaded a script for the current competition"
            )

        script_row = Script(
            script_uuid=script_uuid,
            miner_fk=miner_id,
            request_fk=request_fk,
        )
        session.add(script_row)
        await session.flush()

        session.add(
            MinerUpload(
                script_fk=script_row.id,
                request_fk=request_fk,
                competition_fk=competition_id,
            )
        )
        await session.commit()
        await session.refresh(script_row)
        write_succeeded = True
    except DuplicateMinerUploadError:
        await session.rollback()
        raise
    except BannedMinerUploadError:
        await session.rollback()
        raise
    except SQLAlchemyError:
        await session.rollback()
        logger.exception("script_write_failed", extra={"key": key})
        raise
    except Exception:
        await session.rollback()
        logger.exception("script_write_failed", extra={"key": key})
        raise
    finally:
        if not write_succeeded:
            try:
                await storage.delete(key)
            except Exception:
                logger.exception("script_upload_cleanup_failed", extra={"key": key})

    if script_row is None:
        raise LookupError("Failed to persist miner script")
    return script_row
