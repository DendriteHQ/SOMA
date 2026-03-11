from __future__ import annotations

import asyncio
import logging
import time
import bittensor as bt
from datetime import datetime, timezone
from typing import Any

from app.core.config import settings
from app.core.logging import configure_logging, get_logger

logger = get_logger(__name__)


def _scalar(value: Any) -> int | float | None:
    if value is None:
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, (int, float)):
        return value
    return None


def _tolist(value: Any) -> list[Any] | None:
    if value is None:
        return None
    if hasattr(value, "tolist"):
        try:
            return value.tolist()
        except Exception:
            return None
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return None


def _axon_to_dict(axon: Any) -> dict[str, Any]:
    return {
        "uid": getattr(axon, "uid", None),
        "hotkey": getattr(axon, "hotkey", None),
        "coldkey": getattr(axon, "coldkey", None),
        "ip": getattr(axon, "ip", None),
        "port": getattr(axon, "port", None),
        "protocol": getattr(axon, "protocol", None),
        "version": getattr(axon, "version", None),
        "is_serving": getattr(axon, "is_serving", None),
    }


class MetagraphService:
    def __init__(self) -> None:
        self._subtensor: Any | None = None
        self._metagraph: Any | None = None
        self._task: asyncio.Task[None] | None = None
        self._stop_event: asyncio.Event | None = None
        self._sync_lock: asyncio.Lock | None = None
        self._latest_snapshot: dict[str, Any] | None = None
        self._last_sync_block: int | None = None
        self._last_sync_at: float | None = None
        self._cached_block: int | None = None
        self._last_block_check_at: float | None = None

    @property
    def latest_snapshot(self) -> dict[str, Any] | None:
        return self._latest_snapshot

    def _build_subtensor(self) -> bt.subtensor:
        logging.getLogger("bittensor").setLevel(logging.INFO)
        # Bittensor reconfigures logging; restore app logging settings.
        configure_logging(
            settings.log_level,
            settings.log_levels,
            include_extras=settings.log_include_extras,
            log_dir=settings.log_dir,
            log_file_max_bytes=settings.log_file_max_bytes,
            log_file_backup_count=settings.log_file_backup_count,
        )
        subtensor_cls = getattr(bt, "subtensor", None) or getattr(bt, "Subtensor", None)
        if subtensor_cls is None:
            raise RuntimeError("bittensor Subtensor class not available")
        config_cls = getattr(bt, "Config", None)
        if config_cls is not None:
            config = config_cls()
            subtensor_config = getattr(config, "subtensor", None)
            if subtensor_config is not None:
                if settings.bt_chain_endpoint:
                    subtensor_config.chain_endpoint = settings.bt_chain_endpoint
                if settings.bt_network:
                    subtensor_config.network = settings.bt_network
                try:
                    return subtensor_cls(config=config)
                except TypeError:
                    pass
        if settings.bt_chain_endpoint:
            try:
                return subtensor_cls(chain_endpoint=settings.bt_chain_endpoint)
            except TypeError:
                pass
        if settings.bt_network:
            try:
                return subtensor_cls(network=settings.bt_network)
            except TypeError:
                pass
        return subtensor_cls()

    def _get_current_block(self) -> int | None:
        if self._subtensor is None:
            return None
        now = time.monotonic()
        if (
            self._last_block_check_at is not None
            and (now - self._last_block_check_at) < 12
        ):
            return self._cached_block
        try:
            block = self._subtensor.get_current_block()
        except Exception:
            logger.exception("metagraph_block_fetch_failed")
            return None
        self._last_block_check_at = now
        self._cached_block = int(block) if block is not None else None
        return self._cached_block

    def _should_sync(self, current_block: int | None) -> bool:
        if self._last_sync_block is None:
            return True
        if settings.bt_metagraph_epoch_length <= 0:
            return True
        if current_block is None:
            return True
        return (
            current_block - self._last_sync_block
        ) >= settings.bt_metagraph_epoch_length

    async def start(self) -> None:
        if self._task is not None:
            return
        if self._stop_event is None:
            self._stop_event = asyncio.Event()
        if self._sync_lock is None:
            self._sync_lock = asyncio.Lock()
        logger.info(
            "metagraph_service_starting",
            extra={
                "netuid": settings.bt_netuid,
                "network": settings.bt_network,
                "chain_endpoint": settings.bt_chain_endpoint,
                "init_timeout_secs": settings.bt_metagraph_init_timeout_secs,
            },
        )
        try:
            if self._subtensor is None:
                logger.info("metagraph_service_building_subtensor")
                self._subtensor = await asyncio.wait_for(
                    asyncio.to_thread(self._build_subtensor),
                    timeout=settings.bt_metagraph_init_timeout_secs,
                )
                logger.info(
                    "metagraph_service_subtensor_built",
                    extra={
                        "network": getattr(self._subtensor, "network", None),
                        "chain_endpoint": getattr(
                            self._subtensor, "chain_endpoint", None
                        ),
                    },
                )
            self._metagraph = await asyncio.wait_for(
                asyncio.to_thread(self._subtensor.metagraph, settings.bt_netuid),
                timeout=settings.bt_metagraph_init_timeout_secs,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "metagraph_service_init_timeout",
                extra={
                    "timeout_secs": settings.bt_metagraph_init_timeout_secs,
                },
            )
            return
        except Exception:
            logger.exception("metagraph_service_start_failed")
            return
        self._task = asyncio.create_task(self._run_loop())
        logger.info(
            f"metagraph_service_started, netuid={settings.bt_netuid}",
            extra={
                "netuid": settings.bt_netuid,
                "network": settings.bt_network,
                "chain_endpoint": settings.bt_chain_endpoint,
                "epoch_length": settings.bt_metagraph_epoch_length,
                "sync_secs": settings.bt_metagraph_sync_secs,
                "sync_timeout_secs": settings.bt_metagraph_sync_timeout_secs,
                "initial_sync": "background",
            },
        )

    async def stop(self) -> None:
        if self._task is None:
            return
        if self._stop_event is not None:
            self._stop_event.set()
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None
        self._stop_event = None
        self._sync_lock = None

    async def _run_loop(self) -> None:
        if self._stop_event is None:
            return
        while not self._stop_event.is_set():
            try:
                await self._sync_once()
            except Exception:
                logger.exception("metagraph_sync_failed")
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=settings.bt_metagraph_sync_secs,
                )
            except asyncio.TimeoutError:
                continue

    async def _sync_once(self) -> None:
        if self._metagraph is None or self._subtensor is None:
            return
        if self._sync_lock is None:
            return
        logger.debug("metagraph_sync_attempt")
        current_block = self._get_current_block()
        if not self._should_sync(current_block):
            return
        async with self._sync_lock:
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(self._metagraph.sync, subtensor=self._subtensor),
                    timeout=settings.bt_metagraph_sync_timeout_secs,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "metagraph_sync_timeout",
                    extra={
                        "timeout_secs": settings.bt_metagraph_sync_timeout_secs,
                    },
                )
                return
            snapshot = self._build_snapshot(self._metagraph)
            self._latest_snapshot = snapshot
            self._last_sync_at = time.monotonic()
            snapshot_block = snapshot.get("block")
            if isinstance(snapshot_block, (int, float)):
                self._last_sync_block = int(snapshot_block)
            elif current_block is not None:
                self._last_sync_block = current_block
            logger.info(
                "metagraph_sync_complete",
                extra={"block": snapshot_block},
            )

    def _build_snapshot(self, metagraph: bt.metagraph) -> dict[str, Any]:
        block = _scalar(getattr(metagraph, "block", None))
        snapshot = {
            "netuid": metagraph.netuid,
            "block": block,
            "n": _scalar(getattr(metagraph, "n", None)),
            "synced_at": datetime.now(timezone.utc).isoformat(),
            "hotkeys": _tolist(getattr(metagraph, "hotkeys", None)) or [],
            "coldkeys": _tolist(getattr(metagraph, "coldkeys", None)) or [],
            "uids": _tolist(getattr(metagraph, "uids", None)),
            "axons": [
                _axon_to_dict(axon)
                for axon in list(getattr(metagraph, "axons", None) or [])
            ],
            "stake": _tolist(getattr(metagraph, "S", None)),
            "alpha_stake": _tolist(getattr(metagraph, "alpha_stake", None)),
            "rank": _tolist(getattr(metagraph, "R", None)),
            "trust": _tolist(getattr(metagraph, "T", None)),
            "consensus": _tolist(getattr(metagraph, "C", None)),
            "incentive": _tolist(getattr(metagraph, "I", None)),
            "emission": _tolist(getattr(metagraph, "E", None)),
            "validator_permit": _tolist(getattr(metagraph, "validator_permit", None)),
            "validator_trust": _tolist(getattr(metagraph, "validator_trust", None)),
        }
        return snapshot
