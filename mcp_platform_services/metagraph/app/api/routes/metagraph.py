from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException, Request, status


router = APIRouter(prefix="/api/private/metagraph", tags=["metagraph"])


def _service(request: Request):
    service = getattr(request.app.state, "metagraph_service", None)
    if service is None or not service.is_connected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Metagraph chain connection unavailable",
        )
    return service


@router.get("/snapshot")
async def snapshot(request: Request) -> dict:
    latest_snapshot = _service(request).latest_snapshot
    if not isinstance(latest_snapshot, dict):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Metagraph snapshot not ready",
        )
    return latest_snapshot


@router.get("/economics")
async def economics(request: Request) -> dict[str, float]:
    service = _service(request)
    try:
        registration_cost_tao, alpha_price_tao = await asyncio.gather(
            asyncio.to_thread(service.get_registration_cost_tao),
            asyncio.to_thread(service.get_alpha_price_tao),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Metagraph economics unavailable",
        ) from exc
    return {
        "registration_cost_tao": registration_cost_tao,
        "alpha_price_tao": alpha_price_tao,
    }
