"""
Standalone Sandbox Service

This service runs on a separate machine and handles sandbox execution requests.
It receives code and texts to compress, runs them in isolated Docker containers,
and stores the results in S3.
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

# Add parent directory to path to find mcp_platform module
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI
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
def get_compressed_text_storage() -> CompressedTextStorage:
    """Get or create compressed text storage instance."""
    if not hasattr(app.state, "compressed_text_storage"):
        s3_storage = S3BlobStorage()
        app.state.compressed_text_storage = CompressedTextStorage(s3_storage)
    return app.state.compressed_text_storage


@app.post("/execute_batch", response_model=ExecuteBatchResponse)
async def execute_batch(request: ExecuteBatchRequest) -> ExecuteBatchResponse:
    """Execute a batch of compression tasks in sandbox.
    
    This endpoint:
    1. Receives compression code and texts
    2. Runs them in isolated Docker containers
    3. Saves compressed results to S3
    4. Returns success status
    """
    logger.info(
        "Received batch execution request: batch_id=%s, texts=%d",
        request.batch_id,
        len(request.challenge_texts),
    )
    
    try:
        # Get sandbox executor
        executor = get_sandbox_executor()
        
        # Execute sandbox
        compressed_texts = await executor.execute_batch(
            challenge_code=request.challenge_code,
            challenge_texts=request.challenge_texts,
            compression_ratios=request.compression_ratios,
            timeout_per_task=request.timeout_per_task,
            container_timeout=request.container_timeout,
        )
        
        # Save to S3
        storage = get_compressed_text_storage()
        await storage.save_batch(request.batch_id, compressed_texts)
        
        logger.info(
            "Batch execution completed: batch_id=%s, results=%d",
            request.batch_id,
            len(compressed_texts),
        )
        
        return ExecuteBatchResponse(
            success=True,
            batch_id=request.batch_id,
        )
        
    except Exception as exc:
        logger.error(
            "Batch execution failed: batch_id=%s, error=%s",
            request.batch_id,
            str(exc),
            exc_info=True,
        )
        return ExecuteBatchResponse(
            success=False,
            batch_id=request.batch_id,
            error=str(exc),
        )


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
