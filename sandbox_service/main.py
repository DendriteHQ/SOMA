"""
Standalone Sandbox Service

This service runs on a separate machine and handles sandbox execution requests.
It receives code and texts to compress, runs them in isolated Docker containers,
and stores the results in S3.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import traceback
from pathlib import Path

# Add parent directory to path to find mcp_platform module
sys.path.insert(0, str(Path(__file__).parent.parent))

import httpx
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
from soma_shared.contracts.sandbox.v1.messages import (
    ExecuteBatchRequest,
    ExecuteBatchResponse,
)
from app.sandbox_executor import SandboxExecutor


# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# FastAPI app
app = FastAPI(
    title="Sandbox Service",
    description="Remote sandbox execution service for SOMA platform",
    version="1.0.0",
)


# Sandbox executor
@app.on_event("startup")
async def startup():
    """Initialize sandbox executor on startup."""
    get_sandbox_executor()
    max_concurrent = int(min(12, os.cpu_count() - 1)) 
    app.state.sandbox_semaphore = asyncio.Semaphore(max_concurrent)
    logger.info("Sandbox semaphore initialized with max_concurrent=%d", max_concurrent)


def get_sandbox_executor() -> SandboxExecutor:
    """Get or create sandbox executor instance."""
    if not hasattr(app.state, "sandbox_executor"):
        image = os.getenv("SANDBOX_IMAGE", "sandbox-runner:local")
        force_rebuild = os.getenv("SANDBOX_FORCE_REBUILD", "false").lower() == "true"
        app.state.sandbox_executor = SandboxExecutor(image=image, auto_build=True)
        # Ensure image is built on startup (force rebuild if flag is set)
        app.state.sandbox_executor.ensure_image(force_rebuild=force_rebuild)
        if force_rebuild:
            logger.info("Sandbox image force rebuild completed")
    return app.state.sandbox_executor


@app.post("/execute_batch", response_model=ExecuteBatchResponse)
async def execute_batch(request: ExecuteBatchRequest) -> ExecuteBatchResponse:
    """Execute a batch of compression tasks in sandbox.

    This endpoint:
    1. Fetches the miner's challenge script via a presigned S3 URL (read-only, scoped access)
    2. Runs it in an isolated Docker container
    3. Uploads compressed results via per-task presigned S3 URLs (write-only, scoped access)
    4. Returns success status
    """
    semaphore: asyncio.Semaphore = app.state.sandbox_semaphore
    try:
        async with asyncio.timeout(0):
            await semaphore.acquire()
    except (asyncio.TimeoutError, TimeoutError):
        logger.warning(
            "Sandbox service at capacity, rejecting batch: batch_id=%s",
            request.batch_id,
        )
        raise HTTPException(
            status_code=429,
            detail="Sandbox service is at capacity. Please try again later.",
        )

    logger.info(
        "Received batch execution request: batch_id=%s, texts=%d",
        request.batch_id,
        len(request.challenge_texts),
    )

    try:
        # Fetch miner challenge code via presigned GET URL — no S3 credentials needed.
        async with httpx.AsyncClient() as http_client:
            script_response = await http_client.get(
                request.script_presigned_url,
                follow_redirects=True,
            )
            script_response.raise_for_status()
        challenge_code = script_response.text
        logging.info(
            "Fetched challenge code for batch_id=%s via presigned URL, length=%d",
            request.batch_id,
            len(challenge_code),
        )

        # Get sandbox executor
        executor = get_sandbox_executor()

        # Execute sandbox
        compressed_texts, task_error = await executor.execute_batch(
            challenge_code=challenge_code,
            challenge_texts=request.challenge_texts,
            compression_ratios=request.compression_ratios,
            timeout_per_task=request.timeout_per_task,
            container_timeout=request.container_timeout,
        )

        # Upload each compressed result via its presigned PUT URL — scoped write access only.
        async with httpx.AsyncClient() as http_client:
            for presigned_url, compressed_text in zip(
                request.storage_presigned_urls, compressed_texts
            ):
                put_resp = await http_client.put(
                    presigned_url,
                    content=compressed_text.encode("utf-8"),
                )
                put_resp.raise_for_status()

        logger.info(
            "Batch execution completed: batch_id=%s, results=%d",
            request.batch_id,
            len(compressed_texts),
        )
        if task_error:
            logger.warning(
                "Batch completed with task failures: batch_id=%s\n%s",
                request.batch_id,
                task_error,
            )

        return ExecuteBatchResponse(
            success=True,
            batch_id=request.batch_id,
            error=task_error,
        )


    except Exception as exc:
        tb = traceback.format_exc()
        logger.error(
            "Batch execution failed: batch_id=%s, error=%s\n%s",
            request.batch_id,
            str(exc),
            tb,
        )
        return ExecuteBatchResponse(
            success=False,
            batch_id=request.batch_id,
            error=f"{type(exc).__name__}: {exc}\n\n{tb}",
        )
    finally:
        semaphore.release()


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy", "service": "sandbox"}


if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("SANDBOX_SERVICE_PORT", "8001"))
    host = os.getenv("SANDBOX_SERVICE_HOST", "0.0.0.0")
    
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info",
    )
