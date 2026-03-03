from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import List
import uuid

import httpx

from app.services.blob.compressed_text_storage import CompressedTextStorage


logger = logging.getLogger(__name__)


class RemoteSandboxManager:
    """Remote sandbox manager that delegates execution to a separate sandbox service."""

    def __init__(
        self,
        *,
        sandbox_service_url: str,
        compressed_text_storage: CompressedTextStorage,
        timeout_per_task: float,
        container_timeout_offset: float,
        request_timeout_offset: float,
        max_sandboxes: int = 10,
    ):
        """Initialize remote sandbox manager.
        
        Args:
            sandbox_service_url: Base URL of the sandbox service
            compressed_text_storage: Storage for compressed texts
            timeout_per_task: Timeout for executing one compression task (seconds)
            container_timeout_offset: Extra time for container overhead (seconds)
            request_timeout_offset: Extra time for HTTP request overhead (seconds, must be > container_offset)
            max_sandboxes: Maximum concurrent sandbox operations (for semaphore)
        """
        self.max_sandboxes = max_sandboxes
        self._timeout_per_task = timeout_per_task
        self._container_timeout_offset = container_timeout_offset
        self._request_timeout_offset = request_timeout_offset
        self._sandbox_service_url = sandbox_service_url.rstrip("/")
        self._compressed_text_storage = compressed_text_storage
        self._semaphore = asyncio.Semaphore(max_sandboxes)
        
        # Validate that request timeout offset > container timeout offset
        if request_timeout_offset <= container_timeout_offset:
            raise ValueError(
                f"request_timeout_offset ({request_timeout_offset}s) must be greater than "
                f"container_timeout_offset ({container_timeout_offset}s) for network overhead"
            )

    async def run_batch(
        self,
        *,
        batch_id: str,
        challenge_code: str,
        challenge_texts: list[str],
        compression_ratios: list[float | None],
        acquire_timeout: float = 10.0,
    ) -> list[str]:
        """Execute a batch of challenges on remote sandbox service.
        
        Args:
            batch_id: Unique batch identifier from ChallengeBatch
            challenge_code: Python code to compress texts
            challenge_texts: List of texts to compress
            compression_ratios: Target compression ratios
            ttl: Time-to-live for the operation
            acquire_timeout: Timeout for acquiring semaphore slot
            
        Returns:
            List of compressed texts
            
        Raises:
            RuntimeError: If platform is at capacity or execution fails
        """
        try:
            async with asyncio.timeout(acquire_timeout):
                await self._semaphore.acquire()
        except asyncio.TimeoutError:
            logger.warning(
                "[RemoteSandbox] Platform at capacity - no sandbox slots available within %ss",
                acquire_timeout
            )
            raise RuntimeError(
                f"Platform is at capacity. Maximum {self.max_sandboxes} sandboxes are currently running. "
                "Please try again later."
            )
        
        try:
            return await self._execute_remote_batch(
                batch_id,
                challenge_code,
                challenge_texts,
                compression_ratios,
            )
        except Exception as exc:
            logger.error(
                "[RemoteSandbox] Batch execution failed: %s", exc, exc_info=True
            )
            return [""] * len(challenge_texts)
        finally:
            self._semaphore.release()

    async def _execute_remote_batch(
        self,
        batch_id: str,
        challenge_code: str,
        challenge_texts: list[str],
        compression_ratios: list[float | None],
    ) -> list[str]:
        """Execute batch on remote sandbox service and retrieve results from S3.
        
        Args:
            batch_id: Unique batch identifier from ChallengeBatch
            challenge_code: Python code to compress texts
            challenge_texts: List of texts to compress
            compression_ratios: Target compression ratios
            
        Returns:
            List of compressed texts
        """
        num_tasks = len(challenge_texts)
        
        # Calculate timeouts based on number of tasks
        container_timeout = (self._timeout_per_task * num_tasks) + self._container_timeout_offset
        request_timeout = (self._timeout_per_task * num_tasks) + self._request_timeout_offset
        
        # Prepare request payload
        payload = {
            "batch_id": batch_id,
            "challenge_code": challenge_code,
            "challenge_texts": challenge_texts,
            "compression_ratios": compression_ratios,
            "timeout_per_task": self._timeout_per_task,
            "container_timeout": container_timeout,
        }
        
        logger.info(
            "[RemoteSandbox] Sending batch to sandbox service: batch_id=%s, texts=%d, "
            "timeout_per_task=%ss, container_timeout=%ss, request_timeout=%ss",
            batch_id,
            num_tasks,
            self._timeout_per_task,
            container_timeout,
            request_timeout,
        )
        
        # Send request to sandbox service
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f"{self._sandbox_service_url}/execute_batch",
                    json=payload,
                    timeout=request_timeout,
                )
                response.raise_for_status()
                result = response.json()
                
                if not result.get("success"):
                    error_msg = result.get("error", "Unknown error")
                    logger.error(
                        "[RemoteSandbox] Sandbox service returned error: %s",
                        error_msg,
                    )
                    return [""] * len(challenge_texts)
                
                logger.info(
                    "[RemoteSandbox] Sandbox execution successful: batch_id=%s",
                    batch_id,
                )
                
            except httpx.TimeoutException as exc:
                logger.error(
                    "[RemoteSandbox] Request to sandbox service timed out: %s",
                    exc,
                )
                return [""] * len(challenge_texts)
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "[RemoteSandbox] Sandbox service returned error status %d: %s",
                    exc.response.status_code,
                    exc.response.text,
                )
                return [""] * len(challenge_texts)
            except Exception as exc:
                logger.error(
                    "[RemoteSandbox] Failed to communicate with sandbox service: %s",
                    exc,
                    exc_info=True,
                )
                return [""] * len(challenge_texts)
        
        # Retrieve compressed texts from S3
        try:
            compressed_texts = await self._compressed_text_storage.get_batch(batch_id)
            logger.info(
                "[RemoteSandbox] Retrieved %d compressed texts from storage",
                len(compressed_texts),
            )
            
            # Normalize the result to match expected length
            if len(compressed_texts) < len(challenge_texts):
                compressed_texts.extend([""] * (len(challenge_texts) - len(compressed_texts)))
            elif len(compressed_texts) > len(challenge_texts):
                compressed_texts = compressed_texts[:len(challenge_texts)]
            
            return compressed_texts
            
        except Exception as exc:
            logger.error(
                "[RemoteSandbox] Failed to retrieve compressed texts from storage: %s",
                exc,
                exc_info=True,
            )
            return [""] * len(challenge_texts)
