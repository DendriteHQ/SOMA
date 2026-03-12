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

from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
from soma_shared.contracts.sandbox.v1.messages import (
    ExecuteBatchRequest,
    ExecuteBatchResponse,
)
from app.sandbox_executor import SandboxExecutor
from mcp_platform.app.services.blob.compressed_text_storage import CompressedTextStorage
from mcp_platform.app.services.blob.s3 import S3BlobStorage


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


# Initialize storage
def get_storage() -> CompressedTextStorage:
    """Get or create blob storage instance (used for both scripts and compressed texts)."""
    if not hasattr(app.state, "storage"):
        s3_storage = S3BlobStorage()
        app.state.storage = CompressedTextStorage(s3_storage)
    return app.state.storage


@app.post("/execute_batch", response_model=ExecuteBatchResponse)
async def execute_batch(request: ExecuteBatchRequest) -> ExecuteBatchResponse:
    """Execute a batch of compression tasks in sandbox.
    
    This endpoint:
    1. Fetches the miner's challenge script from S3 using the provided key
    2. Runs them in isolated Docker containers
    3. Saves compressed results to S3
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
        storage = get_storage()
        logging.info("Script S3 key: %s", request.script_s3_key)
        # Fetch miner challenge code from S3
        challenge_code = await storage.get_script(request.script_s3_key)
        logging.info("Fetched challenge code for batch_id=%s, length=%d", request.batch_id, len(challenge_code))
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

        # Save each compressed text individually to S3 using the per-challenge UUID
        for storage_uuid, compressed_text in zip(request.storage_uuids, compressed_texts):
            await storage.save_single(storage_uuid, compressed_text)

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
