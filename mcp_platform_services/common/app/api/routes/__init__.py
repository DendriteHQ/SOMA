"""Router assembly across separately packaged service implementations."""
from importlib import import_module
from pkgutil import extend_path

from fastapi import APIRouter


__path__ = extend_path(__path__, __name__)


ROUTER_MODULES = {
    "frontend": "app.api.routes.frontend",
    "incentive_admin": "app.api.routes.incentive_admin",
    "miner": "app.api.routes.miner",
    "metagraph": "app.api.routes.metagraph",
    "sandbox": "app.api.routes.sandbox",
    "validator": "app.api.routes.validator",
}


PUBLIC_ROUTER_NAMES = ("frontend", "incentive_admin", "miner", "sandbox", "validator")


def build_api_router(names=PUBLIC_ROUTER_NAMES):
    router = APIRouter()
    for name in (ROUTER_MODULES if names is None else names):
        module = import_module(ROUTER_MODULES[name])
        router.include_router(module.router)
    return router


def build_available_public_api_router():
    """Build an OpenAPI router from endpoints included in the current image."""
    router = APIRouter()
    for name in PUBLIC_ROUTER_NAMES:
        try:
            module = import_module(ROUTER_MODULES[name])
        except ModuleNotFoundError as exc:
            if exc.name != ROUTER_MODULES[name]:
                raise
            continue
        router.include_router(module.router)
    return router
