from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from soma_shared.db.models.request import Request
from soma_shared.db.request_metrics import apply_db_metrics_snapshot_to_request
from soma_shared.db.session import get_current_db_request_metrics_snapshot
from soma_shared.db.models.validator import Validator
from soma_shared.db.models.validator_heartbeat import ValidatorHeartbeat
from app.core.logging import get_logger

logger = get_logger(__name__)


async def log_validator_heartbeat(
    session: AsyncSession,
    *,
    request_id: str | None,
    validator_ss58: str,
    status: str,
    version: str | None = None,
    code_changed: bool | None = None,
    model_name: str | None = None,
) -> None:
    try:
        request_fk = None
        request_row: Request | None = None
        if request_id:
            result = await session.execute(
                select(Request).where(Request.external_request_id == request_id)
            )
            request_row = result.scalars().first()
            if request_row is not None:
                request_fk = request_row.id
            else:
                # Create Request record for heartbeat
                request_row = Request(
                    external_request_id=request_id,
                    endpoint="/heartbeat",
                    method="POST",
                    payload={},
                    status_code=200 if status == "working" else None,
                )
                session.add(request_row)
                await session.flush()
                request_fk = request_row.id
        now = datetime.now(timezone.utc)
        result = await session.execute(
            select(Validator).where(Validator.ss58 == validator_ss58)
        )
        validator = result.scalars().first()
        if validator is None:
            validator = Validator(
                ss58=validator_ss58,
                created_at=now,
                last_seen_at=now,
                current_status=status,
                version=version,
                code_changed=code_changed,
            )
            session.add(validator)
            await session.flush()
        else:
            validator.last_seen_at = now
            validator.current_status = status
            if version is not None:
                validator.version = version
            if code_changed is not None:
                validator.code_changed = code_changed
            if model_name is not None:
                validator.model = model_name
        entry = ValidatorHeartbeat(
            request_fk=request_fk,
            validator_fk=validator.id,
            status=status,
        )
        session.add(entry)
        await session.flush()
        if request_row is not None:
            metrics_snapshot = get_current_db_request_metrics_snapshot()
            apply_db_metrics_snapshot_to_request(request_row, metrics_snapshot)
        await session.commit()
    except SQLAlchemyError:
        await session.rollback()
        logger.exception("validator_heartbeat_write_failed")
        raise
