from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.interfaces.query_registry import db_query_interface
from soma_shared.db.models.request import Request as RequestModel


@db_query_interface(sample_kwargs={"request_id": "sample-request-id"})
async def get_request_model_by_external_request_id(
    db: AsyncSession,
    *,
    request_id: str,
) -> RequestModel | None:
    result = await db.execute(
        select(RequestModel).where(RequestModel.external_request_id == request_id)
    )
    return result.scalars().first()
