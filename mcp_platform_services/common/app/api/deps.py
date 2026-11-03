from __future__ import annotations

from functools import lru_cache
from datetime import datetime
from typing import Any
from fastapi import Depends, HTTPException, status
from pydantic import BaseModel
from app.services.blob.base import BlobStorage
from app.services.blob.script_storage import ScriptStorage
from app.services.blob.s3 import S3BlobStorage
from soma_shared.utils.verifier import verify_request_dep, verify_miner_request_dep


@lru_cache(maxsize=1)
def get_blob_storage() -> BlobStorage:
    # Cached because it's stateless; it only holds config/session objects.
    return S3BlobStorage()


@lru_cache(maxsize=1)
def get_script_storage() -> ScriptStorage:
    return ScriptStorage(get_blob_storage())


def _collect_naive_datetimes(value: Any, path: str, errors: list[dict]) -> None:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            errors.append(
                {
                    "loc": path,
                    "msg": "Datetime must include timezone offset (e.g. 'Z' or '+00:00')",
                    "type": "value_error.timezone",
                }
            )
        return

    if isinstance(value, BaseModel):
        data = value.model_dump(mode="python")
        _collect_naive_datetimes(data, path, errors)
        return

    if isinstance(value, dict):
        for key, item in value.items():
            _collect_naive_datetimes(item, f"{path}.{key}", errors)
        return

    if isinstance(value, (list, tuple, set)):
        for idx, item in enumerate(value):
            _collect_naive_datetimes(item, f"{path}[{idx}]", errors)


def _ensure_timezone_payload(payload: Any) -> None:
    errors: list[dict] = []
    _collect_naive_datetimes(payload, "payload", errors)
    if errors:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=errors)


def verify_request_dep_tz(model, expected_key=None):
    base_dep = verify_request_dep(model, expected_key)

    async def _dependency(env=Depends(base_dep)):
        _ensure_timezone_payload(env.payload)
        return env

    return _dependency


def verify_miner_request_dep_tz(model, expected_key=None):
    base_dep = verify_miner_request_dep(model, expected_key)

    async def _dependency(env=Depends(base_dep)):
        _ensure_timezone_payload(env.payload)
        return env

    return _dependency
