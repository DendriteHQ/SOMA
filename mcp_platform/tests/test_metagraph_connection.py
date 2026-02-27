import os
import sys

import pytest
import bittensor as bt

TESTS_DIR = os.path.dirname(__file__)
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if MCP_PLATFORM_DIR not in sys.path:
    sys.path.insert(0, MCP_PLATFORM_DIR)

from app.core.config import settings


def _build_subtensor():
    chain_endpoint = settings.bt_chain_endpoint
    network = settings.bt_network
    subtensor_cls = getattr(bt, "subtensor", None) or getattr(bt, "Subtensor", None)
    if subtensor_cls is None:
        return None
    config_cls = getattr(bt, "Config", None)
    if config_cls is not None:
        config = config_cls()
        subtensor_config = getattr(config, "subtensor", None)
        if subtensor_config is not None:
            if chain_endpoint:
                subtensor_config.chain_endpoint = chain_endpoint
            if network:
                subtensor_config.network = network
            try:
                return subtensor_cls(config=config)
            except TypeError:
                pass
    if chain_endpoint:
        try:
            return subtensor_cls(chain_endpoint=chain_endpoint)
        except TypeError:
            pass
    if network:
        try:
            return subtensor_cls(network=network)
        except TypeError:
            pass
    return subtensor_cls()


def _netuid() -> int:
    return settings.bt_netuid


def _to_list(value):
    if value is None:
        return []
    if hasattr(value, "tolist"):
        return value.tolist()
    return list(value)


@pytest.fixture(scope="module")
def synced_metagraph():
    subtensor = _build_subtensor()
    if subtensor is None:
        pytest.skip("Set BT_CHAIN_ENDPOINT or BT_NETWORK to run.")

    metagraph = subtensor.metagraph(_netuid())
    metagraph.sync(subtensor=subtensor)
    return metagraph


@pytest.mark.network
def test_metagraph_sync(synced_metagraph, capsys):
    block = getattr(synced_metagraph, "block", None)
    n = getattr(synced_metagraph, "n", None)
    hotkeys = list(getattr(synced_metagraph, "hotkeys", []))
    with capsys.disabled():
        print(f"bt_chain_endpoint={settings.bt_chain_endpoint}")
        print(f"bt_network={settings.bt_network}")
        print(f"bt_netuid={settings.bt_netuid}")
        print(f"metagraph_hotkeys_count={len(hotkeys)}")
        if hotkeys:
            print(f"metagraph_hotkey_sample={hotkeys[0]}")

    assert block is not None
    assert n is not None
    assert int(n) >= 0


@pytest.mark.network
def test_metagraph_axons_consistency(synced_metagraph):
    n = getattr(synced_metagraph, "n", None)
    axons = _to_list(getattr(synced_metagraph, "axons", None))
    if n is not None:
        assert len(axons) == int(n)


@pytest.mark.network
def test_metagraph_hotkeys_uids_consistency(synced_metagraph):
    uids = _to_list(getattr(synced_metagraph, "uids", None))
    hotkeys = _to_list(getattr(synced_metagraph, "hotkeys", None))
    if uids:
        assert len(uids) == len(hotkeys)


@pytest.mark.network
def test_metagraph_metrics_lengths(synced_metagraph):
    n = getattr(synced_metagraph, "n", None)
    if n is None:
        pytest.skip("Metagraph missing n")
    n_int = int(n)
    for field in ("S", "R", "T", "C", "I", "E"):
        values = getattr(synced_metagraph, field, None)
        if values is None:
            continue
        values = _to_list(values)
        assert len(values) == n_int
