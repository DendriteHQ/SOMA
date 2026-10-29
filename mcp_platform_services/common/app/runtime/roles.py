from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ServiceRole:
    name: str
    routers: tuple[str, ...]
    metagraph: bool = False
    metagraph_client: bool = False
    database: bool = True
    heartbeat: bool = False
    coordinator: bool = False
    results: bool = False


ROLES = {
    "api": ServiceRole(
        "api", ("miner", "validator"), metagraph_client=True, heartbeat=True,
    ),
    "coordinator": ServiceRole("coordinator", ("sandbox",), coordinator=True),
    "results": ServiceRole(
        "results", ("frontend", "incentive_admin"), metagraph_client=True, results=True,
    ),
    "metagraph": ServiceRole(
        "metagraph", ("metagraph",), metagraph=True, database=False,
    ),
}


def get_role(name: str) -> ServiceRole:
    try:
        return ROLES[name]
    except KeyError as exc:
        raise ValueError(f"Unknown service role {name!r}; expected one of {', '.join(ROLES)}") from exc
