import json

import numpy as np

from validator.weight_history_store import WeightHistoryStore


def test_weight_history_store_persists_latest_snapshot(tmp_path):
    history_file = tmp_path / "weight_history.json"
    store = WeightHistoryStore(str(history_file), max_entries=5)

    store.append(
        np.array([1, 2], dtype=np.int64),
        np.array([0.6, 0.4], dtype=np.float32),
        recorded_at_unix=1000.0,
    )
    store.append(
        np.array([3, 4], dtype=np.int64),
        np.array([0.7, 0.3], dtype=np.float32),
        recorded_at_unix=2000.0,
    )

    reloaded_store = WeightHistoryStore(str(history_file), max_entries=5)
    latest = reloaded_store.latest()
    assert latest is not None
    assert latest.recorded_at_unix == 2000.0
    assert np.array_equal(latest.uids, np.array([3, 4], dtype=np.int64))
    assert np.allclose(latest.weights, np.array([0.7, 0.3], dtype=np.float32))


def test_weight_history_store_respects_max_entries(tmp_path):
    history_file = tmp_path / "weight_history.json"
    store = WeightHistoryStore(str(history_file), max_entries=2)

    store.append(
        np.array([1], dtype=np.int64),
        np.array([1.0], dtype=np.float32),
        recorded_at_unix=10.0,
    )
    store.append(
        np.array([2], dtype=np.int64),
        np.array([1.0], dtype=np.float32),
        recorded_at_unix=20.0,
    )
    store.append(
        np.array([3], dtype=np.int64),
        np.array([1.0], dtype=np.float32),
        recorded_at_unix=30.0,
    )

    payload = json.loads(history_file.read_text(encoding="utf-8"))
    entries = payload["entries"]
    assert len(entries) == 2
    assert entries[0]["recorded_at_unix"] == 20.0
    assert entries[1]["recorded_at_unix"] == 30.0
