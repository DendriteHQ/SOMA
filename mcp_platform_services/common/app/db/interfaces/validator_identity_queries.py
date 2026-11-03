from __future__ import annotations

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.interfaces.query_registry import db_query_interface
from soma_shared.db.models.validator import Validator
from soma_shared.db.models.validator_registration import ValidatorRegistration


@db_query_interface(sample_kwargs={"ss58": "sample-validator-ss58"})
async def get_validator_by_ss58_unarchived(
    db: AsyncSession,
    *,
    ss58: str,
) -> Validator | None:
    result = await db.execute(
        select(Validator)
        .where(Validator.ss58 == ss58)
        .where(Validator.is_archive.is_(False))
    )
    return result.scalars().first()


@db_query_interface(sample_kwargs={"validator_hotkey": "sample-validator-ss58"})
async def get_validator_by_ss58_any(
    db: AsyncSession,
    *,
    validator_hotkey: str,
) -> Validator | None:
    result = await db.execute(select(Validator).where(Validator.ss58 == validator_hotkey))
    return result.scalars().first()


@db_query_interface(sample_kwargs={"validator_id": 0})
async def deactivate_validator_registrations(
    db: AsyncSession,
    *,
    validator_id: int,
) -> int:
    update_result = await db.execute(
        update(ValidatorRegistration)
        .where(ValidatorRegistration.validator_fk == validator_id)
        .values(is_active=False)
    )
    return int(update_result.rowcount or 0)
