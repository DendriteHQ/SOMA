import json
import logging
from logging.handlers import QueueHandler
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
}


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


def _configure_json_logging(
    level: str,
    module_levels: Mapping[str, str] | None,
    include_extras: bool,
) -> None:
    root = logging.getLogger()
    root.setLevel(level.upper())

    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler()
    fmt = jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    handler.setFormatter(fmt)
    _apply_extras_filter(include_extras)
    _apply_render_extras_filter(False)
    root.addHandler(handler)
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
                if not isinstance(handler, QueueHandler):
                    logger.removeHandler(handler)
            continue
        for handler in list(logger.handlers):
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
) -> None:
    global _BT_CONFIGURED
    try:
        import bittensor as bt  # type: ignore
    except Exception:
        _configure_json_logging(level, module_levels, include_extras)
        return

    # Use bittensor logger for application logs.
    _set_bittensor_level(bt, level)
    _enable_bt_source_location(bt)
    if not _BT_CONFIGURED:
        try:
            bt.logging.register_primary_logger("app")
        except Exception:
            _configure_json_logging(level, module_levels, include_extras)
            return
        _BT_CONFIGURED = True

    _clear_non_bt_handlers(bt)
    try:
        bt.logging.enable_third_party_loggers()
        _apply_extras_filter(include_extras)
        _apply_render_extras_filter(include_extras)
        _disable_propagation_for_queue_handlers()
        _apply_module_log_levels(module_levels)
    except Exception:
        _configure_json_logging(level, module_levels, include_extras)
