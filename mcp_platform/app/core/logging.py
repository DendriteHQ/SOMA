import json
import logging
from dataclasses import dataclass
from logging.handlers import QueueHandler, RotatingFileHandler
from pathlib import Path
from typing import Mapping
from pythonjsonlogger import jsonlogger

_BT_CONFIGURED = False
_LOG_RECORD_ATTRS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "message",
    "taskName",
    "task",
    "log_domain",
    "log_component",
}


@dataclass(frozen=True)
class _LoggerRoute:
    module_prefix: str
    logger_name: str
    log_domain: str
    log_component: str


_LOGGER_ROUTES = (
    _LoggerRoute("app.main", "app.lifecycle.main", "lifecycle", "main"),
    _LoggerRoute(
        "app.api.routes.frontend",
        "app.api.frontend",
        "api",
        "frontend",
    ),
    _LoggerRoute("app.api.routes.miner", "app.api.miner", "api", "miner"),
    _LoggerRoute(
        "app.api.routes.validator",
        "app.protocol.validator",
        "protocol",
        "validator",
    ),
    _LoggerRoute(
        "app.api.routes.utils",
        "app.protocol.utils",
        "protocol",
        "utils",
    ),
    _LoggerRoute(
        "app.services.heartbeat",
        "app.jobs.heartbeat",
        "jobs",
        "heartbeat",
    ),
    _LoggerRoute(
        "app.services.batch_cleanup",
        "app.jobs.cleanup",
        "jobs",
        "cleanup",
    ),
    _LoggerRoute(
        "app.services.competition_challenge_activation",
        "app.protocol.challenge_activation",
        "protocol",
        "challenge_activation",
    ),
    _LoggerRoute(
        "app.services.metagraph_runner",
        "app.integration.metagraph.runner",
        "integration",
        "metagraph_runner",
    ),
    _LoggerRoute(
        "app.services.metagraph",
        "app.integration.metagraph",
        "integration",
        "metagraph",
    ),
    _LoggerRoute(
        "app.services.sandbox",
        "app.integration.sandbox",
        "integration",
        "sandbox",
    ),
    _LoggerRoute(
        "app.services.script_store",
        "app.integration.storage",
        "integration",
        "storage",
    ),
    _LoggerRoute(
        "app.services.challenge_factory",
        "app.protocol.challenge_factory",
        "protocol",
        "challenge_factory",
    ),
    _LoggerRoute(
        "app.db.validator_heartbeat_log",
        "app.jobs.heartbeat.persistence",
        "jobs",
        "heartbeat_persistence",
    ),
    _LoggerRoute(
        "app.db.mock_data",
        "app.lifecycle.mock_data",
        "lifecycle",
        "mock_data",
    ),
)

_FILE_LOGGER_ROUTES = {
    "app.lifecycle": Path("lifecycle"),
    "app.api": Path("api"),
    "app.protocol": Path("protocol"),
    "app.jobs.heartbeat": Path("jobs") / "heartbeat",
    "app.jobs.cleanup": Path("jobs") / "cleanup",
    "app.integration.metagraph": Path("integration") / "metagraph",
    "app.integration.sandbox": Path("integration") / "sandbox",
    "app.integration.storage": Path("integration") / "storage",
    "app.security": Path("security"),
}


class _DomainLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        extra = dict(self.extra)
        provided_extra = kwargs.get("extra")
        if isinstance(provided_extra, dict):
            extra.update(provided_extra)
        kwargs["extra"] = extra
        return msg, kwargs


class _ContextDefaultsFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        _, default_domain, default_component = _resolve_logger_context(record.name)
        record.__dict__.setdefault("log_domain", default_domain)
        record.__dict__.setdefault("log_component", default_component)
        return True


class _LevelRangeFilter(logging.Filter):
    def __init__(
        self,
        *,
        min_level: int,
        max_level: int | None = None,
    ) -> None:
        super().__init__()
        self._min_level = min_level
        self._max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno < self._min_level:
            return False
        if self._max_level is not None and record.levelno > self._max_level:
            return False
        return True


class _LoggerPrefixFilter(logging.Filter):
    def __init__(self, logger_prefix: str) -> None:
        super().__init__()
        self._logger_prefix = logger_prefix

    def filter(self, record: logging.LogRecord) -> bool:
        return record.name == self._logger_prefix or record.name.startswith(
            self._logger_prefix + "."
        )


class _ManagedRotatingFileHandler(RotatingFileHandler):
    pass


def _resolve_logger_context(module_name: str) -> tuple[str, str, str]:
    normalized = (module_name or "").strip()
    for route in _LOGGER_ROUTES:
        if normalized == route.module_prefix or normalized.startswith(
            route.module_prefix + "."
        ):
            return route.logger_name, route.log_domain, route.log_component

    if normalized.startswith("app."):
        parts = normalized.split(".")
        log_domain = parts[1] if len(parts) > 1 else "app"
        log_component = "_".join(parts[2:]) if len(parts) > 2 else log_domain
        return normalized, log_domain, log_component or log_domain

    return normalized or "app.unknown", "external", normalized or "unknown"


def get_logger(module_name: str) -> logging.LoggerAdapter:
    logger_name, log_domain, log_component = _resolve_logger_context(module_name)
    return _DomainLoggerAdapter(
        logging.getLogger(logger_name),
        {
            "log_domain": log_domain,
            "log_component": log_component,
        },
    )


class _ExtrasFilter(logging.Filter):
    def __init__(self, include_extras: bool) -> None:
        super().__init__()
        self._include_extras = include_extras

    def filter(self, record: logging.LogRecord) -> bool:
        if self._include_extras:
            return True
        for key in list(record.__dict__.keys()):
            if key not in _LOG_RECORD_ATTRS:
                record.__dict__.pop(key, None)
        return True


class _RenderExtrasFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if getattr(record, "_extras_rendered", False):
            return True
        extras: dict[str, object] = {}
        for key, value in record.__dict__.items():
            if key not in _LOG_RECORD_ATTRS and not key.startswith("_"):
                extras[key] = value
        if not extras:
            return True
        rendered = " ".join(
            f"{key}={_render_extras_value(value)}" for key, value in extras.items()
        )
        message = record.getMessage()
        record.msg = f"{message} {rendered}" if message else rendered
        record.args = ()
        record.__dict__["_extras_rendered"] = True
        for key in extras:
            record.__dict__.pop(key, None)
        return True


def _render_extras_value(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    try:
        return json.dumps(value, ensure_ascii=True, default=str)
    except Exception:
        return str(value)


def _to_python_log_level(level: str) -> int:
    normalized = str(level).strip().upper()
    if normalized == "TRACE":
        return logging.DEBUG
    return logging.getLevelNamesMapping().get(normalized, logging.INFO)


def _build_json_formatter() -> jsonlogger.JsonFormatter:
    return jsonlogger.JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(log_domain)s %(log_component)s %(message)s"
    )


def _create_file_handler(
    log_path: Path,
    *,
    logger_prefix: str,
    min_level: int,
    max_level: int | None,
    max_bytes: int,
    backup_count: int,
) -> _ManagedRotatingFileHandler:
    handler = _ManagedRotatingFileHandler(
        log_path,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    handler.setLevel(min_level)
    handler.setFormatter(_build_json_formatter())
    handler.addFilter(_LoggerPrefixFilter(logger_prefix))
    handler.addFilter(_LevelRangeFilter(min_level=min_level, max_level=max_level))
    handler._mcp_handler_key = (
        str(log_path),
        logger_prefix,
        min_level,
        max_level,
    )
    return handler


def _build_file_handlers_for_prefix(
    log_dir: Path,
    *,
    logger_prefix: str,
    max_bytes: int,
    backup_count: int,
) -> list[logging.Handler]:
    log_dir.mkdir(parents=True, exist_ok=True)
    return [
        _create_file_handler(
            log_dir / "all.log",
            logger_prefix=logger_prefix,
            min_level=logging.DEBUG,
            max_level=None,
            max_bytes=max_bytes,
            backup_count=backup_count,
        ),
        _create_file_handler(
            log_dir / "debug.log",
            logger_prefix=logger_prefix,
            min_level=logging.DEBUG,
            max_level=logging.DEBUG,
            max_bytes=max_bytes,
            backup_count=backup_count,
        ),
        _create_file_handler(
            log_dir / "info.log",
            logger_prefix=logger_prefix,
            min_level=logging.INFO,
            max_level=logging.INFO,
            max_bytes=max_bytes,
            backup_count=backup_count,
        ),
        _create_file_handler(
            log_dir / "warning.log",
            logger_prefix=logger_prefix,
            min_level=logging.WARNING,
            max_level=logging.WARNING,
            max_bytes=max_bytes,
            backup_count=backup_count,
        ),
        _create_file_handler(
            log_dir / "error.log",
            logger_prefix=logger_prefix,
            min_level=logging.ERROR,
            max_level=None,
            max_bytes=max_bytes,
            backup_count=backup_count,
        ),
    ]


def _attach_file_handlers_for_prefix(
    logger: logging.Logger,
    *,
    log_dir: Path,
    logger_prefix: str,
    max_bytes: int,
    backup_count: int,
) -> None:
    existing_handler_keys = {
        getattr(handler, "_mcp_handler_key", None)
        for handler in logger.handlers
        if isinstance(handler, _ManagedRotatingFileHandler)
    }
    for handler in _build_file_handlers_for_prefix(
        log_dir,
        logger_prefix=logger_prefix,
        max_bytes=max_bytes,
        backup_count=backup_count,
    ):
        handler_key = getattr(handler, "_mcp_handler_key", None)
        if handler_key in existing_handler_keys:
            handler.close()
            continue
        logger.addHandler(handler)
        existing_handler_keys.add(handler_key)


def _match_file_logger_prefix(logger_name: str) -> str | None:
    matched_prefixes = [
        prefix
        for prefix in _FILE_LOGGER_ROUTES
        if logger_name == prefix or logger_name.startswith(prefix + ".")
    ]
    if not matched_prefixes:
        return None
    return max(matched_prefixes, key=len)


def _attach_file_handlers(
    logger: logging.Logger,
    *,
    log_dir: Path | None,
    max_bytes: int,
    backup_count: int,
) -> None:
    if log_dir is None:
        return
    for logger_prefix, relative_dir in _FILE_LOGGER_ROUTES.items():
        _attach_file_handlers_for_prefix(
            logger,
            log_dir=log_dir / relative_dir,
            logger_prefix=logger_prefix,
            max_bytes=max_bytes,
            backup_count=backup_count,
        )


def _attach_file_handlers_to_queue_loggers(
    *,
    log_dir: Path | None,
    max_bytes: int,
    backup_count: int,
) -> None:
    if log_dir is None:
        return
    for logger in logging.root.manager.loggerDict.values():
        if not isinstance(logger, logging.Logger):
            continue
        if not any(isinstance(handler, QueueHandler) for handler in logger.handlers):
            continue
        matched_prefix = _match_file_logger_prefix(logger.name)
        if matched_prefix is None:
            continue
        relative_dir = _FILE_LOGGER_ROUTES[matched_prefix]
        _attach_file_handlers_for_prefix(
            logger,
            log_dir=log_dir / relative_dir,
            logger_prefix=matched_prefix,
            max_bytes=max_bytes,
            backup_count=backup_count,
        )


def _configure_json_logging(
    level: str,
    module_levels: Mapping[str, str] | None,
    include_extras: bool,
    log_dir: Path | None = None,
    log_file_max_bytes: int = 10 * 1024 * 1024,
    log_file_backup_count: int = 5,
) -> None:
    root = logging.getLogger()
    root.setLevel(_to_python_log_level(level))

    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler()
    fmt = _build_json_formatter()
    handler.setFormatter(fmt)
    _apply_extras_filter(include_extras)
    _apply_context_defaults_filter()
    _apply_render_extras_filter(False)
    root.addHandler(handler)
    _attach_file_handlers(
        root,
        log_dir=log_dir,
        max_bytes=log_file_max_bytes,
        backup_count=log_file_backup_count,
    )
    _apply_module_log_levels(module_levels)


def _fallback_to_json_logging(
    level: str,
    module_levels: Mapping[str, str] | None,
    include_extras: bool,
    log_dir: Path | None,
    log_file_max_bytes: int,
    log_file_backup_count: int,
) -> None:
    _configure_json_logging(
        level,
        module_levels,
        include_extras,
        log_dir=log_dir,
        log_file_max_bytes=log_file_max_bytes,
        log_file_backup_count=log_file_backup_count,
    )


def _configure_bittensor_logging(
    *,
    include_extras: bool,
    log_dir: Path | None,
    log_file_max_bytes: int,
    log_file_backup_count: int,
    level: str,
    module_levels: Mapping[str, str] | None,
) -> None:
    bt_app_logger = logging.getLogger("app")
    bt_app_logger.setLevel(_to_python_log_level(level))
    _apply_extras_filter(include_extras)
    _apply_context_defaults_filter()
    _apply_render_extras_filter(include_extras)
    _attach_file_handlers(
        bt_app_logger,
        log_dir=log_dir,
        max_bytes=log_file_max_bytes,
        backup_count=log_file_backup_count,
    )
    _attach_file_handlers_to_queue_loggers(
        log_dir=log_dir,
        max_bytes=log_file_max_bytes,
        backup_count=log_file_backup_count,
    )
    _disable_propagation_for_queue_handlers()
    _apply_module_log_levels(module_levels)


def _set_bittensor_level(bt, level: str) -> None:
    level_upper = level.upper()
    if level_upper == "TRACE":
        bt.logging.set_trace()
    elif level_upper == "DEBUG":
        bt.logging.set_debug()
    elif level_upper in {"WARNING", "WARN", "ERROR", "CRITICAL"}:
        bt.logging.set_warning()
    else:
        bt.logging.set_info()


def _enable_bt_source_location(bt) -> None:
    formatter = getattr(bt.logging, "_stream_formatter", None)
    if formatter is not None and hasattr(formatter, "set_trace"):
        formatter.set_trace(True)


def _clear_non_bt_handlers(bt) -> None:
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)

    primary = set(getattr(bt.logging, "_primary_loggers", set()))
    bt_logger = getattr(bt.logging, "_logger", None)
    if bt_logger is not None:
        primary.add(bt_logger.name)

    for logger in logging.root.manager.loggerDict.values():
        if not isinstance(logger, logging.Logger):
            continue
        if logger.name in primary:
            for handler in list(logger.handlers):
                if not isinstance(
                    handler,
                    (QueueHandler, _ManagedRotatingFileHandler),
                ):
                    logger.removeHandler(handler)
            continue
        for handler in list(logger.handlers):
            if isinstance(handler, _ManagedRotatingFileHandler):
                continue
            logger.removeHandler(handler)


def _disable_propagation_for_queue_handlers() -> None:
    for logger in logging.root.manager.loggerDict.values():
        if not isinstance(logger, logging.Logger):
            continue
        if any(isinstance(h, QueueHandler) for h in logger.handlers):
            logger.propagate = False


def _apply_extras_filter(include_extras: bool) -> None:
    _remove_filter_type(_ExtrasFilter)
    if include_extras:
        return
    root = logging.getLogger()
    root.addFilter(_ExtrasFilter(include_extras=False))
    for logger in logging.root.manager.loggerDict.values():
        if not isinstance(logger, logging.Logger):
            continue
        if logger.handlers:
            logger.addFilter(_ExtrasFilter(include_extras=False))


def _apply_render_extras_filter(enable: bool) -> None:
    _remove_filter_type(_RenderExtrasFilter)
    if not enable:
        return
    root = logging.getLogger()
    root.addFilter(_RenderExtrasFilter())
    for logger in logging.root.manager.loggerDict.values():
        if not isinstance(logger, logging.Logger):
            continue
        if logger.handlers:
            logger.addFilter(_RenderExtrasFilter())


def _apply_context_defaults_filter() -> None:
    _remove_filter_type(_ContextDefaultsFilter)
    root = logging.getLogger()
    root.addFilter(_ContextDefaultsFilter())
    for logger in logging.root.manager.loggerDict.values():
        if not isinstance(logger, logging.Logger):
            continue
        if logger.handlers:
            logger.addFilter(_ContextDefaultsFilter())


def _remove_filter_type(filter_type: type[logging.Filter]) -> None:
    root = logging.getLogger()
    for f in list(root.filters):
        if isinstance(f, filter_type):
            root.removeFilter(f)
    for logger in logging.root.manager.loggerDict.values():
        if not isinstance(logger, logging.Logger):
            continue
        for f in list(logger.filters):
            if isinstance(f, filter_type):
                logger.removeFilter(f)


def _apply_module_log_levels(module_levels: Mapping[str, str] | None) -> None:
    if not module_levels:
        return
    normalized: dict[str, str] = {}
    for key, value in module_levels.items():
        if not key:
            continue
        normalized[str(key).strip()] = str(value).strip().upper()

    for name, level in normalized.items():
        logging.getLogger(name).setLevel(level)

    for logger in logging.root.manager.loggerDict.values():
        if not isinstance(logger, logging.Logger):
            continue
        for prefix, level in normalized.items():
            if logger.name == prefix or logger.name.startswith(prefix + "."):
                logger.setLevel(level)


def configure_logging(
    level: str,
    module_levels: Mapping[str, str] | None = None,
    include_extras: bool = True,
    log_dir: str | Path | None = None,
    log_file_max_bytes: int = 10 * 1024 * 1024,
    log_file_backup_count: int = 5,
) -> None:
    global _BT_CONFIGURED
    resolved_log_dir = Path(log_dir).expanduser() if log_dir else None
    try:
        import bittensor as bt  # type: ignore
    except Exception:
        _fallback_to_json_logging(
            level,
            module_levels,
            include_extras,
            resolved_log_dir,
            log_file_max_bytes,
            log_file_backup_count,
        )
        return

    # Use bittensor logger for application logs.
    _set_bittensor_level(bt, level)
    _enable_bt_source_location(bt)
    if not _BT_CONFIGURED:
        try:
            bt.logging.register_primary_logger("app")
        except Exception:
            _fallback_to_json_logging(
                level,
                module_levels,
                include_extras,
                resolved_log_dir,
                log_file_max_bytes,
                log_file_backup_count,
            )
            return
        _BT_CONFIGURED = True

    _clear_non_bt_handlers(bt)
    try:
        bt.logging.enable_third_party_loggers()
        _configure_bittensor_logging(
            include_extras=include_extras,
            log_dir=resolved_log_dir,
            log_file_max_bytes=log_file_max_bytes,
            log_file_backup_count=log_file_backup_count,
            level=level,
            module_levels=module_levels,
        )
    except Exception:
        _fallback_to_json_logging(
            level,
            module_levels,
            include_extras,
            resolved_log_dir,
            log_file_max_bytes,
            log_file_backup_count,
        )
