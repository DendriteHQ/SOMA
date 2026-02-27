from __future__ import annotations

import asyncio
import logging
import sys
import traceback
import uuid
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy import select

from app.core.config import settings
from app.core.logging import configure_logging
from soma_shared.db.session import init_db, close_db, get_db_session, clear_db
from app.db.mock_data import seed_debug_data
from soma_shared.db.models.validator import Validator
from soma_shared.db.models.validator_registration import ValidatorRegistration
from soma_shared.db.models.burn_request import BurnRequest
from app.api.routes import api_router
from soma_shared.utils.signer import get_wallet_from_settings
from app.services.heartbeat import start_heartbeat_thread, stop_heartbeat_thread
from app.services.batch_cleanup import (
    start_batch_cleanup_task,
    stop_batch_cleanup_task,
)
from app.services.metagraph import MetagraphService
from app.services.metagraph_runner import MetagraphServiceRunner

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    configure_logging(
        settings.log_level,
        settings.log_levels,
        include_extras=settings.log_include_extras,
    )

    app = FastAPI(
        title=settings.app_name,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    @app.exception_handler(RequestValidationError)
    async def _request_validation_error_handler(
        request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        request_id = getattr(request.state, "request_id", None)
        try:
            body_bytes = await request.body()
            body_text = body_bytes.decode("utf-8", errors="replace")
        except Exception:
            body_text = "<unavailable>"
        logger.warning(
            "request_validation_error",
            extra={
                "request_id": request_id,
                "endpoint": request.url.path,
                "errors": exc.errors(),
                "body": body_text[:2000],
            },
        )
        return JSONResponse(
            status_code=422,
            content={"detail": exc.errors()},
        )

    async def _load_registered_validators() -> None:
        validators: dict[str, dict[str, object]] = {}
        try:
            async for session in get_db_session():
                result = await session.execute(
                    select(ValidatorRegistration, Validator)
                    .join(Validator, ValidatorRegistration.validator_fk == Validator.id)
                    .where(ValidatorRegistration.is_active.is_(True))
                )
                rows = result.all()
                for registration, validator in rows:
                    validators[validator.ss58] = {
                        "validator_fk": validator.id,
                        "validator_ss58": validator.ss58,
                        "request_fk": registration.request_fk,
                        "ip": registration.ip or validator.ip,
                        "port": registration.port or validator.port,
                        "registered_at": registration.registered_at,
                    }
        except Exception:
            logger.exception("registered_validators_load_failed")
            validators = {}
        app.state.registered_validators = validators
        logger.info(
            "registered_validators_loaded",
            extra={"count": len(validators)},
        )

    def _log_startup_failure(step: str, exc: BaseException) -> None:
        configure_logging(
            settings.log_level,
            settings.log_levels,
            include_extras=settings.log_include_extras,
        )
        logger.exception("startup_failed", extra={"step": step})
        traceback.print_exception(type(exc), exc, exc.__traceback__, file=sys.stderr)

    @app.on_event("startup")
    async def _startup() -> None:
        app.state.main_loop = asyncio.get_running_loop()
        logger.info(
            "startup_config",
            extra={
                "app_env": settings.app_env,
                "log_level": settings.log_level,
                "debug": settings.debug,
                "inject_mock_data": settings.inject_mock_data,
                "bt_netuid": settings.bt_netuid,
                "bt_network": settings.bt_network,
                "bt_chain_endpoint": settings.bt_chain_endpoint,
                "bt_metagraph_epoch_length": settings.bt_metagraph_epoch_length,
                "bt_metagraph_sync_secs": settings.bt_metagraph_sync_secs,
                "bt_metagraph_sync_timeout_secs": settings.bt_metagraph_sync_timeout_secs,
                "bt_metagraph_init_timeout_secs": settings.bt_metagraph_init_timeout_secs,
                "wallet_name": settings.wallet_name,
                "wallet_hotkey": settings.wallet_hotkey,
                "wallet_path": settings.wallet_path,
            },
        )

        from app.api.routes.utils import _get_nlp

        _get_nlp()
        try:
            dsn = settings.get_postgres_dsn()
            if not dsn:
                raise RuntimeError(
                    "Database DSN not configured (set POSTGRES_DSN or RDS_SECRET_ID)"
                )
            await init_db(
                dsn=dsn,
                echo=settings.db_echo,
                pool_size=settings.db_pool_size,
                max_overflow=settings.db_max_overflow,
            )
        except BaseException as exc:
            _log_startup_failure("init_db", exc)
            raise

        if settings.debug:
            if settings.debug_clear_db:
                try:
                    await clear_db()
                except BaseException as exc:
                    _log_startup_failure("clear_db", exc)
                    raise
            if settings.inject_mock_data:
                try:
                    async for session in get_db_session():
                        await seed_debug_data(session)
                        break
                except BaseException as exc:
                    _log_startup_failure("seed_debug_data", exc)
                    raise
            app.state.burn = False
            app.state.burn_ratio = 1.0
            logger.info("burn_state_initialized", extra={"burn_active": False})
        else:
            # Initialize burn state from database
            try:
                async for session in get_db_session():
                    result = await session.execute(
                        select(BurnRequest)
                        .order_by(BurnRequest.created_at.desc())
                        .limit(1)
                    )
                    latest_burn = result.scalars().first()
                    if latest_burn:
                        app.state.burn = latest_burn.is_active
                        app.state.burn_ratio = latest_burn.burn_ratio
                    else:
                        app.state.burn = True
                        app.state.burn_ratio = 1.0
                    logger.info(
                        "burn_state_initialized",
                        extra={"burn_active": app.state.burn},
                    )
                    break
            except Exception:
                logger.warning("burn_state_init_failed, defaulting to False")
                app.state.burn = False
                app.state.burn_ratio = 1.0
        try:
            app.state.metagraph_service = MetagraphService()
            app.state.metagraph_runner = MetagraphServiceRunner(
                app.state.metagraph_service
            )
            app.state.metagraph_runner.start()
        except BaseException as exc:
            _log_startup_failure("metagraph_start", exc)
            raise
        try:
            await _load_registered_validators()
        except BaseException as exc:
            _log_startup_failure("load_registered_validators", exc)
            raise
        try:
            wallet = get_wallet_from_settings()
        except BaseException as exc:
            _log_startup_failure("wallet_load", exc)
            raise
        configure_logging(
            settings.log_level,
            settings.log_levels,
            include_extras=settings.log_include_extras,
        )
        hot_ss58 = None
        try:
            hot_ss58 = wallet.hotkey.ss58_address
        except Exception:
            pass
        logger.info(
            "wallet_loaded",
            extra={
                "wallet_name": settings.wallet_name,
                "wallet_hotkey": settings.wallet_hotkey,
                "wallet_path": settings.wallet_path,
                "hot_ss58": hot_ss58,
            },
        )
        try:
            start_heartbeat_thread(app)
        except BaseException as exc:
            _log_startup_failure("heartbeat_start", exc)
            raise
        try:
            start_batch_cleanup_task(app)
        except BaseException as exc:
            _log_startup_failure("batch_cleanup_start", exc)
            raise
        logger.info("startup_complete", extra={"env": settings.app_env})

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        logger.info("shutdown_start")
        metagraph_runner = getattr(app.state, "metagraph_runner", None)
        if metagraph_runner is not None:
            metagraph_runner.stop()

        sandbox_manager = getattr(app.state, "sandbox_manager", None)
        if sandbox_manager is not None:
            try:
                await asyncio.to_thread(sandbox_manager.shutdown)
            except Exception:
                logger.exception("sandbox_manager_shutdown_failed")

        stop_heartbeat_thread(app)
        await stop_batch_cleanup_task(app)
        await close_db()
        logger.info("shutdown_complete")

    @app.middleware("http")
    async def add_request_id(request: Request, call_next):
        # Stateless-friendly: no session, just request-scoped metadata if needed
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        logger.exception("unhandled_exception", extra={"path": str(request.url)})
        return JSONResponse(
            status_code=500, content={"detail": "Internal Server Error"}
        )

    app.include_router(api_router)
    return app


app = create_app()
