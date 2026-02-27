from fastapi import APIRouter
from app.api.routes.frontend import router as frontend_router
from app.api.routes.health import router as health_router
from app.api.routes.miner import router as miner_router
from app.api.routes.validator import router as validator_router

api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(frontend_router)
api_router.include_router(miner_router)
api_router.include_router(validator_router)
