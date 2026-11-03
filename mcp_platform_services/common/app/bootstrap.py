from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager
from urllib.parse import urlsplit
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.openapi.utils import get_openapi
from sqlalchemy import select, text
from sqlalchemy.exc import OperationalError

from app.core.config import settings
from app.db.session import close_reader_db, get_db_read_session, init_reader_db
from app.core.logging import configure_logging, get_logger
from soma_shared.db.session import (
    begin_db_request_metrics_scope,
    close_db,
    end_db_request_metrics_scope,
    get_db_session,
    init_db,
)
from app.core.validation_logging import loggable_validation_errors
from soma_shared.db.models.validator import Validator
from soma_shared.db.models.validator_registration import ValidatorRegistration
from app.api.routes import build_api_router, build_available_public_api_router
from app.runtime.roles import get_role
from app.runtime.database import connect_existing_database
from soma_shared.utils.signer import get_wallet_from_settings
from app.services.metagraph_client import RemoteMetagraphService

logger = get_logger(__name__)


# Keep these adapters at the composition boundary. They defer loading code owned
# by another service package until that role is selected, while also making the
# lifecycle dependencies straightforward to replace in isolated tests.
def MetagraphService():
    from app.services.metagraph import MetagraphService as implementation

    return implementation()


def MetagraphServiceRunner(service):
    from app.services.metagraph_runner import MetagraphServiceRunner as implementation

    return implementation(service)


def start_heartbeat_thread(app) -> None:
    from app.services.heartbeat import start_heartbeat_thread as implementation

    implementation(app)


def stop_heartbeat_thread(app) -> None:
    from app.services.heartbeat import stop_heartbeat_thread as implementation

    implementation(app)


def start_dockerhub_visibility_task(app) -> None:
    from app.services.dockerhub_visibility import start_dockerhub_visibility_task as implementation

    implementation(app)


async def stop_dockerhub_visibility_task(app) -> None:
    from app.services.dockerhub_visibility import stop_dockerhub_visibility_task as implementation

    await implementation(app)


def start_swebench_orchestrator_task(app) -> None:
    from app.services.swebench_orchestrator import start_swebench_orchestrator_task as implementation

    implementation(app)


async def stop_swebench_orchestrator_task(app) -> None:
    from app.services.swebench_orchestrator import stop_swebench_orchestrator_task as implementation

    await implementation(app)


def start_latest_competition_aggregate_refresh_task(app) -> None:
    from app.api.routes.frontend import start_latest_competition_aggregate_refresh_task as implementation

    implementation(app)


async def stop_latest_competition_aggregate_refresh_task(app) -> None:
    from app.api.routes.frontend import stop_latest_competition_aggregate_refresh_task as implementation

    await implementation(app)


def _iter_exception_chain(exc: BaseException):
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        yield current
        seen.add(id(current))
        next_exc = (
            current.__cause__
            if current.__cause__ is not None
            else current.__context__
        )
        current = next_exc if isinstance(next_exc, BaseException) else None


def _is_db_connectivity_error(exc: BaseException) -> bool:
    connectivity_types = (TimeoutError, ConnectionError, OSError, OperationalError)
    for chain_exc in _iter_exception_chain(exc):
        if isinstance(chain_exc, connectivity_types):
            return True
        msg = str(chain_exc).lower()
        if "connect" in msg and (
            "timeout" in msg
            or "refused" in msg
            or "unreachable" in msg
            or "network" in msg
        ):
            return True
    return False


def _db_target_from_dsn(dsn: str | None) -> dict[str, object]:
    if not dsn:
        return {}
    try:
        parsed = urlsplit(dsn)
    except Exception:
        return {}
    db_name = parsed.path.lstrip("/") or None
    return {
        "db_scheme": parsed.scheme or None,
        "db_host": parsed.hostname,
        "db_port": parsed.port,
        "db_name": db_name,
    }


def create_app(role_name: str | None = None) -> FastAPI:
    role = get_role(role_name or settings.service_role)
    configure_logging(
        settings.log_level,
        settings.log_levels,
        include_extras=settings.log_include_extras,
        log_dir=settings.log_dir,
        log_file_max_bytes=settings.log_file_max_bytes,
        log_file_backup_count=settings.log_file_backup_count,
    )

    app = FastAPI(
        title=settings.app_name,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )
    app.state.service_role = role.name
    app.state.ready = False

    @app.exception_handler(RequestValidationError)
    async def _request_validation_error_handler(
        request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        request_id = getattr(request.state, "request_id", None)
        logger.warning(
            "request_validation_error",
            extra={
                "request_id": request_id,
                "endpoint": request.url.path,
                "errors": loggable_validation_errors(exc.errors()),
            },
        )
        return JSONResponse(
            status_code=422,
            content={"detail": exc.errors()},
        )

    async def _load_registered_validators() -> None:
        validators: dict[str, dict[str, object]] = {}
        try:
            async for session in get_db_read_session():
                result = await session.execute(
                    select(ValidatorRegistration, Validator)
                    .join(Validator, ValidatorRegistration.validator_fk == Validator.id)
                    .where(ValidatorRegistration.is_active.is_(True))
                    .where(Validator.is_archive.is_(False))
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

    def _log_startup_failure(
        step: str,
        exc: BaseException,
        *,
        include_traceback: bool = True,
        event: str = "startup_failed",
        extra: dict[str, object] | None = None,
    ) -> None:
        configure_logging(
            settings.log_level,
            settings.log_levels,
            include_extras=settings.log_include_extras,
            log_dir=settings.log_dir,
            log_file_max_bytes=settings.log_file_max_bytes,
            log_file_backup_count=settings.log_file_backup_count,
        )
        log_extra: dict[str, object] = {
            "step": step,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
        if extra:
            log_extra.update(extra)
        if include_traceback:
            logger.exception(event, extra=log_extra)
        else:
            logger.error(event, extra=log_extra)

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

        if role.database:
            from app.api.routes.utils import _get_nlp

            _get_nlp()
        if role.database and (settings.debug_clear_db or settings.inject_mock_data):
            raise RuntimeError("Split services cannot clear or seed the shared database")
        if role.database and settings.db_initialize_schema:
            raise RuntimeError("Split services require DB_INITIALIZE_SCHEMA=false; bootstrap/migrate separately")
        dsn: str | None = None
        db_target: dict[str, object] = {}
        async def _init_db_once() -> None:
            initialize = init_db if settings.db_initialize_schema else connect_existing_database
            await initialize(
                dsn=dsn,
                echo=settings.db_echo,
                pool_size=settings.db_pool_size,
                max_overflow=settings.db_max_overflow,
            )

        if role.database:
            try:
                dsn = settings.get_postgres_writer_dsn()
                if not dsn:
                    raise RuntimeError(
                        "Database DSN not configured (set POSTGRES_DSN or RDS_SECRET_ID)"
                    )
                db_target = _db_target_from_dsn(dsn)
                await _init_db_once()
            except BaseException as exc:
                if _is_db_connectivity_error(exc):
                    _log_startup_failure(
                        "init_db",
                        exc,
                        include_traceback=False,
                        event="database_connection_unavailable",
                        extra=db_target,
                    )
                    raise RuntimeError(
                        "Database connection unavailable during startup."
                    ) from None
                _log_startup_failure("init_db", exc)
                raise

            try:
                await init_reader_db()
            except BaseException as exc:
                _log_startup_failure(
                    "init_reader_db",
                    exc,
                    event="reader_database_unavailable",
                    extra={"fallback": "writer"},
                )

        if role.metagraph:
            try:
                app.state.metagraph_service = MetagraphService()
                app.state.metagraph_runner = MetagraphServiceRunner(app.state.metagraph_service)
                app.state.metagraph_runner.start()
            except BaseException as exc:
                _log_startup_failure("metagraph_start", exc)
                raise
        if role.metagraph_client:
            try:
                app.state.metagraph_client = RemoteMetagraphService(
                    settings.metagraph_service_url,
                    refresh_secs=settings.metagraph_client_refresh_secs,
                    timeout_secs=settings.metagraph_client_timeout_secs,
                )
                app.state.metagraph_service = app.state.metagraph_client
                app.state.metagraph_client.start()
            except BaseException as exc:
                _log_startup_failure("metagraph_client_start", exc)
                raise
        if role.heartbeat:
            await _load_registered_validators()
            # Signing is needed by the API/heartbeat process, not dispatch/results.
            get_wallet_from_settings()
            start_heartbeat_thread(app)
        if role.coordinator:
            # Publisher snapshots are process-local. Keep this loop with dispatch
            # until task readiness has an external durable representation.
            start_dockerhub_visibility_task(app)
            start_swebench_orchestrator_task(app)
        if role.results:
            start_latest_competition_aggregate_refresh_task(app)
        app.state.ready = True
        logger.info("startup_complete", extra={"env": settings.app_env})

    async def _shutdown() -> None:
        app.state.ready = False
        logger.info("shutdown_start")
        metagraph_runner = getattr(app.state, "metagraph_runner", None)
        if metagraph_runner is not None:
            metagraph_runner.stop()
        metagraph_client = getattr(app.state, "metagraph_client", None)
        if metagraph_client is not None:
            metagraph_client.stop()

        sandbox_manager = getattr(app.state, "sandbox_manager", None)
        if sandbox_manager is not None:
            try:
                shutdown = getattr(sandbox_manager, "shutdown", None)
                if callable(shutdown):
                    await asyncio.to_thread(shutdown)
                else:
                    logger.warning("sandbox_manager_has_no_shutdown")
            except Exception:
                logger.exception("sandbox_manager_shutdown_failed")

        if role.heartbeat:
            stop_heartbeat_thread(app)
        if role.coordinator:
            await stop_swebench_orchestrator_task(app)
            await stop_dockerhub_visibility_task(app)
        if role.results:
            await stop_latest_competition_aggregate_refresh_task(app)
        if role.database:
            await close_reader_db()
            await close_db()
        logger.info("shutdown_complete")

    @asynccontextmanager
    async def lifespan(_app):
        try:
            await _startup()
            yield
        finally:
            await _shutdown()

    app.router.lifespan_context = lifespan

    @app.middleware("http")
    async def add_request_id(request: Request, call_next):
        # Stateless-friendly: no session, just request-scoped metadata if needed
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        request.state.request_id = request_id
        metrics_token = begin_db_request_metrics_scope()
        try:
            response = await call_next(request)
        finally:
            end_db_request_metrics_scope(metrics_token)
        response.headers["X-Request-ID"] = request_id
        return response

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        logger.exception("unhandled_exception", extra={"path": str(request.url)})
        return JSONResponse(
            status_code=500, content={"detail": "Internal Server Error"}
        )

    @app.get("/health", include_in_schema=False)
    async def health():
        return {"status": "ok", "service": role.name}

    @app.get("/health/ready", include_in_schema=False)
    async def ready():
        if not app.state.ready:
            return JSONResponse(status_code=503, content={"status": "starting"})
        for name in (
            "swebench_orchestrator_task", "dockerhub_visibility_task",
            "latest_competition_aggregate_refresh_task",
        ):
            task = getattr(app.state, name, None)
            if task is not None and task.done():
                return JSONResponse(status_code=503, content={"status": "worker_stopped", "worker": name})
        if role.database:
            try:
                async for db in get_db_session():
                    await db.execute(text("SELECT 1"))
                    break
            except Exception:
                return JSONResponse(status_code=503, content={"status": "database_unavailable"})
        if role.metagraph or role.metagraph_client:
            metagraph_service = getattr(app.state, "metagraph_service", None)
            if (
                metagraph_service is None
                or not metagraph_service.is_connected
                or not isinstance(metagraph_service.latest_snapshot, dict)
            ):
                return JSONResponse(status_code=503, content={"status": "metagraph_unavailable"})
        return {"status": "ready", "service": role.name}

    app.include_router(build_api_router(role.routers))
    if role.name == "api":
        # Ingress serves the original combined API documentation on port 8000.
        # Runtime routes still belong to exactly one process.
        def combined_openapi():
            if app.openapi_schema is None:
                app.openapi_schema = get_openapi(
                    title=app.title,
                    version=app.version,
                    routes=build_available_public_api_router().routes,
                )
            return app.openapi_schema
        app.openapi = combined_openapi
    return app
