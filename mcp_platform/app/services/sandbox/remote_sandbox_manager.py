from __future__ import annotations

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
    ):
        """Initialize remote sandbox manager.
        
        Args:
            sandbox_service_url: Base URL of the sandbox service
            compressed_text_storage: Storage for compressed texts
            timeout_per_task: Timeout for executing one compression task (seconds)
            container_timeout_offset: Extra time for container overhead (seconds)
            request_timeout_offset: Extra time for HTTP request overhead (seconds, must be > container_offset)
        """
        self._timeout_per_task = timeout_per_task
        self._container_timeout_offset = container_timeout_offset
        self._request_timeout_offset = request_timeout_offset
        self._sandbox_service_url = sandbox_service_url.rstrip("/")
        self._compressed_text_storage = compressed_text_storage
        
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
        script_s3_key: str,
        challenge_texts: list[str],
        compression_ratios: list[float | None],
        storage_uuids: list[str],
    ) -> list[str]:
        """Execute a batch of challenges on remote sandbox service.
        
        Args:
            batch_id: Unique batch identifier from ChallengeBatch (for logging)
            script_s3_key: S3 key of the miner's challenge script
            challenge_texts: List of texts to compress
            compression_ratios: Target compression ratios
            storage_uuids: S3 storage UUIDs, one per challenge_text entry
            
        Returns:
            List of compressed texts
            
        Raises:
            RuntimeError: If platform is at capacity
        """
        try:
            return await self._execute_remote_batch(
                batch_id,
                script_s3_key,
                challenge_texts,
                compression_ratios,
                storage_uuids,
            )
        except RuntimeError:
            raise
        except Exception as exc:
            logger.error(
                "[RemoteSandbox] Batch execution failed: %s", exc, exc_info=True
            )
            return [""] * len(challenge_texts)

    async def _execute_remote_batch(
        self,
        batch_id: str,
        script_s3_key: str,
        challenge_texts: list[str],
        compression_ratios: list[float | None],
        storage_uuids: list[str],
    ) -> list[str]:
        """Execute batch on remote sandbox service and retrieve results from S3.
        
        Args:
            batch_id: Unique batch identifier from ChallengeBatch (for logging)
            script_s3_key: S3 key of the miner's challenge script
            challenge_texts: List of texts to compress
            compression_ratios: Target compression ratios
            storage_uuids: S3 storage UUIDs, one per challenge_text entry
            
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
            "script_s3_key": script_s3_key,
            "challenge_texts": challenge_texts,
            "compression_ratios": compression_ratios,
            "storage_uuids": storage_uuids,
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
                if exc.response.status_code == 429:
                    logger.warning(
                        "[RemoteSandbox] Sandbox service at capacity (429): batch_id=%s",
                        batch_id,
                    )
                    raise RuntimeError(
                        "Platform is at capacity. The sandbox service is currently handling the maximum "
                        "number of concurrent requests. Please try again later."
                    )
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
        
        # Retrieve individual compressed texts from S3 using per-challenge UUIDs
        compressed_texts: list[str] = []
        for storage_uuid in storage_uuids:
            try:
                text = await self._compressed_text_storage.get_single(storage_uuid)
                compressed_texts.append(text)
            except Exception as exc:
                logger.error(
                    "[RemoteSandbox] Failed to retrieve compressed text for uuid=%s: %s",
                    storage_uuid,
                    exc,
                    exc_info=True,
                )
                compressed_texts.append("")

        logger.info(
            "[RemoteSandbox] Retrieved %d compressed texts from storage",
            len(compressed_texts),
        )
        return compressed_texts

    def shutdown(self) -> None:
        """Compatibility lifecycle hook used by app shutdown."""
        logger.info("[RemoteSandbox] Shutdown complete")
