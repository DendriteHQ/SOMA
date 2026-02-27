import os
import sys
import uuid

import pytest

TESTS_DIR = os.path.dirname(__file__)
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if MCP_PLATFORM_DIR not in sys.path:
    sys.path.insert(0, MCP_PLATFORM_DIR)


def _load_env_from_file() -> None:
    env_path = os.path.join(MCP_PLATFORM_DIR, ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key:
                os.environ.setdefault(key, value)


_load_env_from_file()

from app.core.config import settings
from app.services.blob.base import BlobStorage
from app.services.blob.s3 import S3BlobStorage
from app.services.blob.script_storage import ScriptStorage


def _ensure_s3_bucket() -> str | None:
    if settings.s3_bucket:
        return settings.s3_bucket
    env_bucket = os.environ.get("S3_BUCKET")
    if env_bucket:
        settings.s3_bucket = env_bucket
    return settings.s3_bucket


class _MemoryBlobStorage(BlobStorage):
    def __init__(self) -> None:
        self.data: dict[str, tuple[bytes, str | None]] = {}
        self.calls: list[tuple[str, str]] = []

    async def put_bytes(
        self, key: str, data: bytes, content_type: str | None = None
    ) -> None:
        self.calls.append(("put", key))
        self.data[key] = (data, content_type)

    async def get_bytes(self, key: str) -> bytes:
        self.calls.append(("get", key))
        return self.data[key][0]

    async def delete(self, key: str) -> None:
        self.calls.append(("delete", key))
        self.data.pop(key, None)


@pytest.fixture
def memory_storage() -> _MemoryBlobStorage:
    return _MemoryBlobStorage()


def test_hot_key_default_prefix(memory_storage):
    storage = ScriptStorage(memory_storage)
    key = storage.hot_key("miner123", "script-1")
    assert key == "hot/miner_solutions/miner123/script-1.py"


def test_hot_key_with_date_prefix(memory_storage):
    storage = ScriptStorage(memory_storage)
    key = storage.hot_key("miner123", "script-1", date_prefix="2025-01-02")
    assert key == "hot/miner_solutions/miner123/2025-01-02/script-1.py"


def test_archive_key_default_prefix(memory_storage):
    storage = ScriptStorage(memory_storage)
    key = storage.archive_key("miner123", "script-1")
    assert key == "archive/miner123/script-1.py"


def test_archive_key_with_date_prefix(memory_storage):
    storage = ScriptStorage(memory_storage)
    key = storage.archive_key("miner123", "script-1", date_prefix="2025-01-02")
    assert key == "archive/2025-01-02/miner123/script-1.py"


def test_prefixes_are_stripped(memory_storage):
    storage = ScriptStorage(
        memory_storage,
        hot_prefix="/hot/",
        archive_prefix="/archive/",
    )
    hot_key = storage.hot_key("miner123", "script-1")
    archive_key = storage.archive_key("miner123", "script-1")
    assert hot_key == "hot/miner_solutions/miner123/script-1.py"
    assert archive_key == "archive/miner123/script-1.py"


@pytest.mark.anyio
async def test_put_get_hot_script_roundtrip(memory_storage):
    storage = ScriptStorage(memory_storage)
    key = await storage.put_hot_script("miner123", "script-1", "print('hi')")
    assert key in memory_storage.data
    data, content_type = memory_storage.data[key]
    assert data == b"print('hi')"
    assert content_type == "text/x-python"
    script = await storage.get_hot_script("miner123", "script-1")
    assert script == "print('hi')"


@pytest.mark.anyio
async def test_put_get_archive_script_roundtrip(memory_storage):
    storage = ScriptStorage(memory_storage)
    key = await storage.put_archive_script("miner123", "script-1", "print('hi')")
    assert key in memory_storage.data
    data, content_type = memory_storage.data[key]
    assert data == b"print('hi')"
    assert content_type == "text/x-python"
    script = await storage.get_archive_script("miner123", "script-1")
    assert script == "print('hi')"


@pytest.mark.anyio
async def test_delete_removes_key(memory_storage):
    storage = ScriptStorage(memory_storage)
    key = await storage.put_hot_script("miner123", "script-1", "print('hi')")
    await storage.delete(key)
    assert key not in memory_storage.data


def test_s3_blob_storage_requires_bucket(monkeypatch):
    monkeypatch.setattr(settings, "s3_bucket", None)
    with pytest.raises(RuntimeError):
        S3BlobStorage()


@pytest.mark.network
@pytest.mark.anyio
async def test_s3_blob_storage_roundtrip():
    if not _ensure_s3_bucket():
        pytest.skip("S3_BUCKET not configured")
    try:
        import boto3
    except ImportError:
        pytest.skip("boto3 is required for AWS credential discovery")
    session = boto3.Session()
    if session.get_credentials() is None:
        pytest.skip("AWS credentials not configured")
    if session.region_name is None:
        pytest.skip("AWS region not configured")

    storage = S3BlobStorage()
    key = f"tests/blob/{uuid.uuid4()}.txt"
    data = b"blob-storage-test"
    await storage.put_bytes(key, data, content_type="text/plain")
    try:
        fetched = await storage.get_bytes(key)
        assert fetched == data
    finally:
        await storage.delete(key)
