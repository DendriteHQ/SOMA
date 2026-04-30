from __future__ import annotations

import importlib
import inspect
import pkgutil
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

_QUERY_META_ATTR = "__db_query_interface_meta__"


@dataclass(frozen=True)
class DBQueryInterfaceMeta:
    threshold_seconds: float
    sample_kwargs_factory: Callable[[], dict[str, Any]]


@dataclass(frozen=True)
class DBQueryInterfaceSpec:
    module_name: str
    function_name: str
    qualified_name: str
    function: Callable[..., Awaitable[Any]]
    threshold_seconds: float
    sample_kwargs_factory: Callable[[], dict[str, Any]]

    def sample_kwargs(self) -> dict[str, Any]:
        return dict(self.sample_kwargs_factory())


def _build_default_sample_kwargs_factory(
    fn: Callable[..., Awaitable[Any]],
) -> Callable[[], dict[str, Any]]:
    signature = inspect.signature(fn)

    def _factory() -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        missing: list[str] = []
        for param in signature.parameters.values():
            if param.name == "db":
                continue
            if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                continue
            if param.default is inspect.Parameter.empty:
                missing.append(param.name)
                continue
            kwargs[param.name] = param.default
        if missing:
            missing_args = ", ".join(missing)
            raise ValueError(
                f"Missing sample args for {fn.__module__}.{fn.__name__}: {missing_args}. "
                "Add defaults or set sample_kwargs/sample_kwargs_factory in @db_query_interface."
            )
        return kwargs

    return _factory


def db_query_interface(
    *,
    threshold_seconds: float = 2.0,
    sample_kwargs: dict[str, Any] | None = None,
    sample_kwargs_factory: Callable[[], dict[str, Any]] | None = None,
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    """
    Mark an async DB-interface function as a benchmarkable query interface.

    The timing tests auto-discover every async function in app/db/interfaces that takes `db`.
    To keep that reliable for future interfaces, each query function should use this decorator.
    """

    def _decorator(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        if not inspect.iscoroutinefunction(fn):
            raise TypeError(
                f"@db_query_interface can only be used on async functions: {fn.__name__}"
            )

        if sample_kwargs_factory is not None and sample_kwargs is not None:
            raise ValueError(
                "Use either sample_kwargs_factory or sample_kwargs, not both."
            )

        if sample_kwargs_factory is not None:
            factory = sample_kwargs_factory
        elif sample_kwargs is not None:
            def _static_factory() -> dict[str, Any]:
                return dict(sample_kwargs)
            factory = _static_factory
        else:
            factory = _build_default_sample_kwargs_factory(fn)

        setattr(
            fn,
            _QUERY_META_ATTR,
            DBQueryInterfaceMeta(
                threshold_seconds=float(threshold_seconds),
                sample_kwargs_factory=factory,
            ),
        )
        return fn

    return _decorator


def _iter_interface_modules(package_name: str) -> list[Any]:
    package = importlib.import_module(package_name)
    modules = [package]
    if not hasattr(package, "__path__"):
        return modules
    for mod in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
        modules.append(importlib.import_module(mod.name))
    return modules


def discover_db_query_interfaces(
    package_name: str = "app.db.interfaces",
) -> tuple[list[DBQueryInterfaceSpec], list[str]]:
    """
    Returns:
      - list of registered query interface specs
      - list of async `db` functions missing @db_query_interface metadata
    """
    specs: list[DBQueryInterfaceSpec] = []
    missing_registration: list[str] = []
    seen: set[str] = set()

    modules = _iter_interface_modules(package_name)
    for module in modules:
        for _, fn in inspect.getmembers(module, inspect.iscoroutinefunction):
            if not callable(fn):
                continue
            try:
                signature = inspect.signature(fn)
            except (TypeError, ValueError):
                continue
            if "db" not in signature.parameters:
                continue
            meta = getattr(fn, _QUERY_META_ATTR, None)
            qualified_name = f"{fn.__module__}.{fn.__name__}"
            if qualified_name in seen:
                continue
            seen.add(qualified_name)
            if meta is None:
                missing_registration.append(qualified_name)
                continue
            specs.append(
                DBQueryInterfaceSpec(
                    module_name=fn.__module__,
                    function_name=fn.__name__,
                    qualified_name=qualified_name,
                    function=fn,
                    threshold_seconds=float(meta.threshold_seconds),
                    sample_kwargs_factory=meta.sample_kwargs_factory,
                )
            )

    specs.sort(key=lambda x: x.qualified_name)
    missing_registration.sort()
    return specs, missing_registration
