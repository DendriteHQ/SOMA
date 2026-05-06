from fastapi import APIRouter
from app.api.routes.frontend import (
    api_key_router as frontend_api_key_router,
    router as frontend_router,
)
from app.api.routes.miner import router as miner_router
from app.api.routes.validator import router as validator_router

api_router = APIRouter()
api_router.include_router(frontend_router)
api_router.include_router(frontend_api_key_router)
api_router.include_router(miner_router)
api_router.include_router(validator_router)
