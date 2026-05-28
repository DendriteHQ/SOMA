"""Miner compression service — wraps base_miner.py as a FastAPI HTTP service.

The miner code (base_miner.py) is volume-mounted at runtime to the path given by
MINER_MODULE_PATH (default /app/miner/base_miner.py). The service calls the miner
as a subprocess using the same stdin/stdout JSON protocol that the openclaw plugin
uses when calling it directly.

API
---
GET  /health  → {"status": "ok"} (503 if miner module not found)
POST /compress → CompressResponse
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

logger = logging.getLogger("miner_service")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

MINER_MODULE_PATH = Path(os.getenv("MINER_MODULE_PATH", "/app/miner/base_miner.py"))
PLUGIN_ID = "soma-miner"
PLUGIN_NAME = "SOMA Miner"

app = FastAPI(
    title="SOMA Miner Service",
    description=(
        "Wraps base_miner.py as a FastAPI service for trajectory compression. "
        "The miner code is volume-mounted at container start time."
    ),
    version="0.1.0",
)


class CompressRequest(BaseModel):
    messages: list[dict[str, Any]] = Field(default_factory=list)
    session_id: str | None = None
    session_key: str | None = None
    current_token_count: int | None = None


@app.get("/health")
def health() -> JSONResponse:
    if not MINER_MODULE_PATH.is_file():
        return JSONResponse(
            {"status": "degraded", "reason": f"miner module not found at {MINER_MODULE_PATH}"},
            status_code=503,
        )
    return JSONResponse({"status": "ok"})


@app.post("/compress")
def compress(request: CompressRequest) -> JSONResponse:
    if not request.messages:
        return JSONResponse({"compress": False})

    # Build the payload in the format the miner subprocess expects (same as index.js buildPayload).
    payload = {
        "pluginId": PLUGIN_ID,
        "pluginName": PLUGIN_NAME,
        "pluginDir": str(MINER_MODULE_PATH.parent),
        "params": {
            "messages": request.messages,
            "sessionId": request.session_id,
            "sessionKey": request.session_key,
            "currentTokenCount": request.current_token_count,
        },
        "sourceHook": "assemble",
    }

    timeout_seconds = float(os.getenv("MINER_SUBPROCESS_TIMEOUT_SECONDS", "120") or "120")
    try:
        result = subprocess.run(
            [sys.executable, str(MINER_MODULE_PATH), "assemble"],
            input=json.dumps(payload),
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired:
        logger.error(
            "Miner subprocess timed out after %.0fs: session_id=%s",
            timeout_seconds,
            request.session_id,
        )
        return JSONResponse({"compress": False})
    except Exception as exc:
        logger.error("Miner subprocess failed to launch: %s", exc)
        return JSONResponse({"compress": False})

    if result.returncode != 0:
        logger.error(
            "Miner subprocess exited with code %s: stderr=%s",
            result.returncode,
            (result.stderr or "").strip()[:500],
        )
        return JSONResponse({"compress": False})

    raw_stdout = (result.stdout or "").strip()
    try:
        parsed = json.loads(raw_stdout or "{}")
    except json.JSONDecodeError as exc:
        logger.error(
            "Failed to parse miner subprocess output: %s stdout_prefix=%s",
            exc,
            raw_stdout[:200],
        )
        return JSONResponse({"compress": False})

    if parsed.get("ok") is False:
        logger.warning(
            "Miner subprocess reported failure: error=%s session_id=%s",
            parsed.get("error"),
            request.session_id,
        )
        return JSONResponse({"compress": False})

    trajectory = parsed.get("result")
    if not isinstance(trajectory, dict):
        return JSONResponse({"compress": False})

    # Check whether the miner indicated it made changes (mirrors shouldUseMinerTrajectory in index.js).
    base_miner = trajectory.get("baseMiner") or {}
    compaction = trajectory.get("compaction") or {}
    changed = (
        base_miner.get("changed") is True
        or base_miner.get("pruned") is True
        or compaction.get("compacted") is True
        or trajectory.get("compacted") is True
    )
    if not changed:
        return JSONResponse({"compress": False})

    compressed_messages = trajectory.get("messages")
    if not isinstance(compressed_messages, list) or not compressed_messages:
        return JSONResponse({"compress": False})

    logger.info(
        "Miner compressed trajectory: session_id=%s input_messages=%s output_messages=%s",
        request.session_id,
        len(request.messages),
        len(compressed_messages),
    )
    return JSONResponse({"compress": True, "trajectory": trajectory})
