from __future__ import annotations
import sys
from pathlib import Path
import os

ROOT = Path(__file__).resolve().parent.parent
MCP_PLATFORM_DIR = ROOT / "mcp_platform"
if str(MCP_PLATFORM_DIR) not in sys.path:
    sys.path.insert(0, str(MCP_PLATFORM_DIR))

from datetime import datetime, timezone
import time
import numpy as np
from fastapi import FastAPI, Depends, HTTPException, Request
from validator.abstract_validator import AbstractValidator
from soma_shared.contracts.validator.v1.messages import (
    GetChallengesRequest,
    HeartbeatRequest,
    HeartbeatResponse,
    PostChallengeScores,
    PostChallengeScoresResponse,
    ScoreSubmissionType,
    ValidatorRegisterRequest,
    ValidatorRegisterResponse,
    GetBestMinersUidRequest,
    GetBestMinersUidResponse,
    GetChallengesResponse,
)
from soma_shared.contracts.common.signatures import SignedEnvelope
import asyncio
from validator.config.settings import Settings
from soma_shared.utils.verifier import verify_request_dep_no_db
import logging
import httpx
from soma_shared.utils.signer import sign_payload_model, generate_nonce
from validator.chain.weigt_setter import WeightSetter
from validator.evaluation.evaluator import BatchScoringError, Evaluator
from validator.evaluation.llm_scorer import LLMClient, LLMInsufficientFundsError
from soma_shared.utils.verifier import verify_httpx_response
import bittensor as bt


def configure_logging() -> None:
    root = logging.getLogger()
    if root.handlers:
        return
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


class Validator(AbstractValidator):
    def __init__(self):
        super().__init__()
        self.settings = self.init_settings()
        self.verify_openrouter_startup()
        self._last_fetch_cause = "unknown"
        self._provider_degraded_until = 0.0
        self.evaluator = Evaluator(settings=self.settings)
        self.weight_setter = WeightSetter(
            netuid=self.settings.netuid, subtensor=self.settings.subtensor
        )
        self.client = None
        resp = asyncio.run(self.register_to_platform())
        self.registered = bool(resp and getattr(resp, "ok", False))
        if not self.registered:
            raise RuntimeError("Validator registration to platform failed.")
        
    def verify_openrouter_startup(self) -> None:
        token = (self.settings.openrouter_api_token or "").strip()
        if not token:
            raise RuntimeError("OPENROUTER_API_TOKEN is required for validator scoring")
        masked_token = (
            f"{token[:8]}...{token[-6:]}" if len(token) > 14 else "***masked***"
        )
        logging.info(
            "OpenRouter startup verification using API token fragment: %s",
            masked_token,
        )

        llm_client = LLMClient(
            url=self.settings.openrouter_api_url,
            api_token=token,
            model=self.settings.openrouter_model,
            timeout_seconds=min(self.settings.llm_timeout_seconds, 20.0),
            max_tokens=min(self.settings.llm_max_tokens, 120),
            temperature=0.0,
        )

        prompt = (
            "Write a short 3-sentence story about a validator node keeping the network healthy."
        )

        try:
            probe_response = asyncio.run(llm_client.ask(prompt))
            status_code = (
                probe_response.get("status_code")
                if isinstance(probe_response, dict)
                else None
            )
            response_text = (
                probe_response.get("text", "")
                if isinstance(probe_response, dict)
                else str(probe_response)
            )
            logging.info(
                "OpenRouter startup verification response: status=%s text=%s",
                status_code,
                response_text[:500],
            )
            if not response_text.strip():
                raise RuntimeError(
                    "OpenRouter startup verification failed: empty response text"
                )
            logging.info("OpenRouter startup verification passed")
        except LLMInsufficientFundsError as exc:
            raise RuntimeError(
                "OpenRouter startup verification failed: insufficient funds"
            ) from exc
        except Exception as exc:
            raise RuntimeError(
                f"OpenRouter startup verification failed: {exc}"
            ) from exc

    def init_settings(self) -> Settings:
        return Settings.from_env()
    
    async def async_init(self) -> None:
        """Initialize async resources in the correct event loop"""
        if self.client is None:
            self.client = httpx.AsyncClient(timeout=self.settings.http_timeout_seconds)
            logging.info("HTTP client initialized")

    async def register_to_platform(self) -> ValidatorRegisterResponse:
        try:
            payload = ValidatorRegisterRequest(
                validator_hotkey=self.settings.wallet.hotkey.ss58_address,
                serving_ip=self.settings.validator_host,
                serving_port=self.settings.validator_port,
            )
            logging.info(
                f"Registration payload created: validator_hotkey={payload.validator_hotkey}, serving_ip={payload.serving_ip}, serving_port={payload.serving_port}"
            )

            nonce = generate_nonce()
            signature = sign_payload_model(
                payload=payload,
                nonce=nonce,
                use_coldkey=False,
                wallet=self.settings.wallet,
            )
            signed_payload = SignedEnvelope(
                payload=payload,
                sig=signature,
            )

            logging.info(
                f"Signed envelope created: payload={signed_payload.payload.model_dump()}, sig.signer_ss58={signed_payload.sig.signer_ss58}, sig.nonce={signed_payload.sig.nonce}"
            )
            logging.info(f"Full request dict: {signed_payload.model_dump()}")
            logging.info(
                f"Registering validator with hotkey: {self.settings.wallet.hotkey}"
            )
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    response = await client.post(
                        f"{self.settings.platform_url}/validator/register",
                        json=signed_payload.model_dump(),
                    )
                    response.raise_for_status()
                    try:
                        signed = verify_httpx_response(
                            response,
                            ValidatorRegisterResponse,
                            expected_key=self.settings.platform_signer_ss58,
                        )
                    except ValueError as verify_exc:
                        logging.warning(
                            f"register_to_platform: signature verification failed: {verify_exc}"
                        )
                        raise
                    logging.info(
                        "Successfully registered validator",
                        extra={"payload": signed.payload.model_dump(mode="json")},
                    )
                    return signed.payload
            except httpx.HTTPError as e:
                logging.error(f"Failed to register validator: {e}", exc_info=True)
                return None
        except Exception as e:
            logging.error(
                f"Exception during validator registration: {e}", exc_info=True
            )
            return None

    async def get_best_miners(self) -> GetBestMinersUidResponse:
        try:
            logging.info("get_best_miners: Creating payload")
            payload = GetBestMinersUidRequest()
            nonce = generate_nonce()
            logging.info("get_best_miners: Signing payload")
            signature = sign_payload_model(
                payload=payload,
                nonce=nonce,
                use_coldkey=False,
                wallet=self.settings.wallet,
            )
            signed_payload = SignedEnvelope(
                payload=payload,
                sig=signature,
            )
            logging.info("Requesting best miners from platform")
            try:
                logging.info(
                    f"get_best_miners: Sending POST to {self.settings.platform_url}/validator/get_best_miners"
                )
                response = await self.client.post(
                    f"{self.settings.platform_url}/validator/get_best_miners",
                    json=signed_payload.model_dump(),
                )
                logging.info(
                    f"get_best_miners: Got response status={response.status_code}"
                )
                response.raise_for_status()
                logging.info("get_best_miners: Response OK, verifying signature")
                try:
                    signed = verify_httpx_response(
                        response,
                        GetBestMinersUidResponse,
                        expected_key=self.settings.platform_signer_ss58,
                    )
                    logging.info("get_best_miners: Signature verified")
                except ValueError as verify_exc:
                    logging.warning(
                        f"get_best_miners: signature verification failed: {verify_exc}"
                    )
                    raise
                except Exception as verify_exc:
                    logging.error(
                        f"get_best_miners: verify_httpx_response failed: {verify_exc}",
                        exc_info=True,
                    )
                    raise
                logging.info(
                    "Fetched best miners",
                    extra={
                        "miners": [(m.uid, m.weight) for m in signed.payload.miners]
                    },
                )
                logging.info("get_best_miners: Returning payload")
                return signed.payload
            except httpx.HTTPError as e:
                logging.error(f"Failed to fetch best miners (HTTP): {e}", exc_info=True)
                return None
        except Exception as e:
            logging.error(f"Exception during get_best_miners: {e}", exc_info=True)
            return None

    async def set_weights(self) -> None:
        try:
            logging.info("set_weights: Starting weight setting process")
            # Get best miners with their weights from platform
            # Platform decides how many miners to return based on its configuration
            best_miners_response = await self.get_best_miners()
            logging.info(
                f"set_weights: Got best_miners_response: {best_miners_response}"
            )

            if not best_miners_response or not best_miners_response.miners:
                logging.warning(
                    "No miners returned from platform setting weight to uid 0"
                )
                await self.weight_setter.set_weights(
                    np.array([0], dtype=np.int64), np.array([1.0], dtype=np.float32)
                )
                return

            # Extract UIDs and weights into numpy arrays
            uids = np.array(
                [m.uid for m in best_miners_response.miners], dtype=np.int64
            )
            weights = np.array(
                [m.weight for m in best_miners_response.miners], dtype=np.float32
            )

            logging.info(
                f"Setting weights for {len(uids)} miners: UIDs={uids.tolist()}, weights={weights.tolist()}"
            )

            await self.weight_setter.set_weights(uids, weights)

        except Exception as e:
            logging.error(f"Exception during setting weights: {e}", exc_info=True)
            raise  # Re-raise to see if this is causing the crash

    async def get_tasks_for_eval(self) -> GetChallengesResponse | None:
        try:
            payload = GetChallengesRequest()
            nonce = generate_nonce()
            signature = sign_payload_model(
                payload=payload, nonce=nonce, wallet=self.settings.wallet
            )
            signed_payload = SignedEnvelope(
                payload=payload,
                sig=signature,
            )
            logging.info(f"Requesting challenges from platform")
            try:
                logging.info(
                    f"Sending POST to {self.settings.platform_url}/validator/request_challenge"
                )
                response = await self.client.post(
                    f"{self.settings.platform_url}/validator/request_challenge",
                    json=signed_payload.model_dump(),
                )
                logging.info(f"Got response: status={response.status_code}")
                logging.info("get_tasks_for_eval: Checking response status")

                # Handle 503 - No tasks available (all miners scored)
                if response.status_code == 503:
                    try:
                        error_detail = response.json().get("detail", "")
                        cause = self._classify_503_cause(error_detail)
                        self._last_fetch_cause = cause
                        if cause == "compression_ratio_all_failed":
                            logging.info(
                                "Platform reports all challenges failed compression ratio check. "
                                "Validator will retry later with compression backoff."
                            )
                            logging.info(
                                "get_tasks_for_eval: Returning None (503 compression ratio all failed)"
                            )
                            return None
                        if cause == "no_tasks":
                            logging.info(
                                "Platform reports no tasks available - all miners are scored. "
                                "Validator will retry later with backoff."
                            )
                            logging.info("get_tasks_for_eval: Returning None (503)")
                            return None
                        logging.info(
                            f"get_tasks_for_eval: Returning None (503 other cause: {cause})"
                        )
                        return None
                    except Exception as e503:
                        logging.error(
                            f"get_tasks_for_eval: Error parsing 503 response: {e503}",
                            exc_info=True,
                        )
                        self._last_fetch_cause = "service_unavailable"
                        pass

                response.raise_for_status()
                logging.info(f"Platform key {self.settings.platform_signer_ss58}")
                logging.info("get_tasks_for_eval: Verifying response")

                try:
                    signed = verify_httpx_response(
                        response,
                        GetChallengesResponse,
                        expected_key=self.settings.platform_signer_ss58,
                    )
                    logging.info(
                        "get_tasks_for_eval: Response verified successfully"
                    )
                except ValueError as verify_exc:
                    logging.warning(
                        f"get_tasks_for_eval: signature verification failed: {verify_exc}"
                    )
                    raise
                except Exception as verify_exc:
                    logging.error(
                        f"get_tasks_for_eval: verify_httpx_response failed: {verify_exc}",
                        exc_info=True,
                    )
                    raise

                # Log compressed challenge payload from platform
                if hasattr(signed.payload, "challenges"):
                    logging.info(f"========== PLATFORM RESPONSE ==========")
                    for idx, c in enumerate(signed.payload.challenges):
                        logging.info(
                            f"  Challenge {idx}: id={c.batch_challenge_id}, "
                            f"text_len={len(c.compressed_text)}, "
                            f"questions={len(c.challenge_questions)}"
                        )

                logging.info(
                    "Fetched challenges",
                    extra={"payload": signed.payload.model_dump(mode="json")},
                )
                self._last_fetch_cause = "ok"
                logging.info("get_tasks_for_eval: Returning payload")
                return signed.payload
            except httpx.HTTPError as e:
                logging.error(
                    f"Failed to fetch challenges (HTTP): {e}\n"
                    f"Status code: {getattr(e.response, 'status_code', 'N/A')}\n"
                    f"Response headers: {getattr(e.response, 'headers', {})}\n"
                    f"Response text: {getattr(e.response, 'text', 'N/A')}",
                    exc_info=True,
                )
                self._last_fetch_cause = "http_error"
                logging.info("get_tasks_for_eval: Returning None (HTTP error)")
                return None
        except Exception as e:
            logging.error(f"Exception during get_tasks_for_eval: {e}", exc_info=True)
            self._last_fetch_cause = "exception"
            logging.info("get_tasks_for_eval: Returning None (exception)")
            return None

    @staticmethod
    def _classify_503_cause(error_detail: str) -> str:
        detail = (error_detail or "").lower()
        if "compression ratio check" in detail:
            return "compression_ratio_all_failed"
        if "no tasks available" in detail:
            return "no_tasks"
        return "service_unavailable"

    @staticmethod
    def _compute_backoff_interval(
        *,
        streak: int,
        base_poll_interval: float,
        backoff_multiplier: float,
        max_backoff_interval: float,
        exponential_attempts: int = 3,
    ) -> float:
        safe_streak = max(0, streak)
        safe_base = max(0.1, base_poll_interval)
        safe_max = max(safe_base, max_backoff_interval)

        if safe_streak == 0:
            return safe_base

        exp_attempts = max(1, exponential_attempts)
        if safe_streak <= exp_attempts:
            interval = safe_base * (backoff_multiplier ** safe_streak)
            return min(interval, safe_max)

        exp_end_interval = safe_base * (backoff_multiplier ** exp_attempts)
        linear_steps = safe_streak - exp_attempts
        interval = exp_end_interval + (linear_steps * safe_base)
        return min(interval, safe_max)

    @staticmethod
    def _loop_tick_interval(base_poll_interval: float) -> float:
        return max(0.5, min(base_poll_interval, 1.0))

    def _resolve_scoring_error_cooldown_seconds(self, error_code: str) -> float:
        if error_code == "provider_insufficient_funds":
            return max(30.0, self.settings.llm_provider_error_cooldown_seconds)
        if error_code.startswith("provider_"):
            return max(30.0, self.settings.llm_scoring_error_cooldown_seconds)
        if error_code == "validator_scoring_failed":
            return max(30.0, self.settings.llm_scoring_error_cooldown_seconds)
        return 0.0

    async def report_results(self, task, results):
        try:
            payload = PostChallengeScores(
                batch_id=task.batch_id, question_scores=results["question_scores"]
            )
            nonce = generate_nonce()
            signature = sign_payload_model(
                payload=payload, nonce=nonce, wallet=self.settings.wallet
            )
            signed_payload = SignedEnvelope(
                payload=payload,
                sig=signature,
            )
            logging.info(
                f"Reporting payload {payload} question scores to platform for batch_id: {task.batch_id}"
            )
            try:
                response = await self.client.post(
                    f"{self.settings.platform_url}/validator/score_challenges",
                    json=signed_payload.model_dump(),
                )
                response.raise_for_status()
                try:
                    signed = verify_httpx_response(
                        response,
                        PostChallengeScoresResponse,
                        expected_key=self.settings.platform_signer_ss58,
                    )
                except ValueError as verify_exc:
                    logging.warning(
                        f"report_results: signature verification failed: {verify_exc}"
                    )
                    raise
                logging.info(
                    "Successfully reported results",
                    extra={"payload": signed.payload.model_dump(mode="json")},
                )
            except httpx.HTTPError as e:
                logging.error(f"Failed to report results: {e}", exc_info=True)
        except Exception as e:
            logging.error(f"Exception during reporting results: {e}", exc_info=True)

    async def report_batch_error(
        self,
        task: GetChallengesResponse,
        *,
        error_code: str,
        error_message: str,
        error_details: dict | None = None,
        retryable: bool = True,
    ) -> None:
        try:
            payload = PostChallengeScores(
                batch_id=task.batch_id,
                question_scores=[],
                submission_type=ScoreSubmissionType.ERROR,
                error_code=error_code,
                error_message=error_message,
                error_details=error_details,
                retryable=retryable,
            )
            nonce = generate_nonce()
            signature = sign_payload_model(
                payload=payload, nonce=nonce, wallet=self.settings.wallet
            )
            signed_payload = SignedEnvelope(payload=payload, sig=signature)
            logging.warning(
                "Reporting batch error for batch_id=%s code=%s retryable=%s",
                task.batch_id,
                error_code,
                retryable,
            )
            response = await self.client.post(
                f"{self.settings.platform_url}/validator/score_challenges",
                json=signed_payload.model_dump(),
            )
            response.raise_for_status()
            verify_httpx_response(
                response,
                PostChallengeScoresResponse,
                expected_key=self.settings.platform_signer_ss58,
            )
            logging.info(
                "Successfully reported batch error for batch_id=%s code=%s",
                task.batch_id,
                error_code,
            )
        except Exception as exc:
            logging.error(
                "Failed to report batch error for batch_id=%s code=%s: %s",
                getattr(task, "batch_id", "unknown"),
                error_code,
                exc,
                exc_info=True,
            )

    def has_eval_capacity(self) -> bool:
        return self.evaluator.has_eval_capacity()

    async def run(self) -> None:
        # Initialize async resources in the correct event loop
        await self.async_init()
        
        base_poll_interval = self.settings.task_poll_interval_seconds
        max_backoff_interval = self.settings.max_backoff_interval_seconds
        backoff_multiplier = self.settings.backoff_multiplier

        current_poll_interval = base_poll_interval
        consecutive_no_tasks = 0
        consecutive_ratio_failures = 0
        fetch_cooldown_until = 0.0

        in_flight: set[asyncio.Task] = set()
        last_weight_set_block: int | None = None
        weight_task: asyncio.Task | None = None

        async def process_task(task: GetChallengesResponse) -> None:
            try:
                results = await self.evaluator.evaluate(task)
                # logging.info(f"Async evaluation results: {results}")
                if results:
                    logging.info(f"Reporting results for task {task.batch_id}")
                    await self.report_results(task, results)
            except BatchScoringError as exc:
                await self.report_batch_error(
                    task,
                    error_code=exc.error_code,
                    error_message=str(exc),
                    error_details=exc.details,
                    retryable=exc.retryable,
                )
                cooldown = self._resolve_scoring_error_cooldown_seconds(exc.error_code)
                if cooldown > 0:
                    self._provider_degraded_until = max(
                        self._provider_degraded_until,
                        time.monotonic() + cooldown,
                    )
                    logging.warning(
                        "Fetch cooldown activated for %.1fs due to scoring error code=%s",
                        cooldown,
                        exc.error_code,
                    )
            except Exception as exc:
                logging.error(
                    f"Failed to process task {getattr(task, 'batch_id', 'unknown')}: {exc}",
                    exc_info=True,
                )
                await self.report_batch_error(
                    task,
                    error_code="validator_processing_error",
                    error_message=f"Task processing failed: {exc}",
                    error_details={"error": str(exc)},
                    retryable=True,
                )

        try:
            logging.info("Validator run loop started")
            while True:
                now = time.monotonic()

                try:
                    current_block = await self.settings.subtensor.get_current_block()
                    if last_weight_set_block is None:
                        last_weight_set_block = current_block
                        logging.info(f"Initialized last_weight_set_block={current_block}")
                    blocks_since_last = current_block - last_weight_set_block
                    if blocks_since_last >= self.settings.weight_block_interval:
                        if weight_task is None or weight_task.done():
                            logging.info(
                                f"Block {current_block}: {blocks_since_last} blocks since last weight set "
                                f"(interval={self.settings.weight_block_interval}), setting weights"
                            )
                            weight_task = asyncio.create_task(self.set_weights())
                            last_weight_set_block = current_block
                except Exception as block_exc:
                    logging.warning(f"Failed to fetch current block: {block_exc}", exc_info=True)

                in_flight = {task for task in in_flight if not task.done()}
                has = self.has_eval_capacity()
                max_in_flight = self.settings.max_concurrent_evaluations
                fetch_due = now >= fetch_cooldown_until
                provider_ready = now >= self._provider_degraded_until
                can_fetch = (
                    has
                    and len(in_flight) < max_in_flight
                    and fetch_due
                    and provider_ready
                )
                cooldown_remaining = max(0.0, fetch_cooldown_until - now)
                provider_cooldown_remaining = max(
                    0.0, self._provider_degraded_until - now
                )
                logging.info(
                    f"Has evaluation capacity: {has}, in_flight tasks: {len(in_flight)}, "
                    f"max_in_flight: {max_in_flight}, can_fetch: {can_fetch}, "
                    f"ratio_fail_streak: {consecutive_ratio_failures}, "
                    f"fetch_due: {fetch_due}, cooldown_remaining: {cooldown_remaining:.1f}s, "
                    f"provider_ready: {provider_ready}, "
                    f"provider_cooldown_remaining: {provider_cooldown_remaining:.1f}s"
                )
                if can_fetch:
                    logging.info("Fetching tasks from platform...")
                    task = await self.get_tasks_for_eval()
                    logging.info(f"Got task: {task}")
                    # logging.info(f"Fetched task: {task}")
                    if task and getattr(task, "challenges", None):
                        # Successfully got tasks - reset backoff
                        consecutive_no_tasks = 0
                        consecutive_ratio_failures = 0
                        current_poll_interval = base_poll_interval
                        fetch_cooldown_until = now
                        logging.info(
                            f"Fetched task: {task.batch_id}, reset poll interval to {current_poll_interval}s"
                        )
                        in_flight.add(asyncio.create_task(process_task(task)))
                    elif task is None:
                        cause = getattr(self, "_last_fetch_cause", "unknown")
                        if cause == "compression_ratio_all_failed":
                            consecutive_ratio_failures += 1
                            consecutive_no_tasks = 0
                            current_poll_interval = base_poll_interval
                            logging.info(
                                "All challenges failed compression ratio check "
                                f"(attempt {consecutive_ratio_failures}), retrying immediately"
                            )
                            fetch_cooldown_until = now
                        else:
                            # No tasks available (503 response) - apply standard backoff
                            consecutive_no_tasks += 1
                            consecutive_ratio_failures = 0
                            current_poll_interval = self._compute_backoff_interval(
                                streak=consecutive_no_tasks,
                                base_poll_interval=base_poll_interval,
                                backoff_multiplier=backoff_multiplier,
                                max_backoff_interval=max_backoff_interval,
                            )
                            logging.info(
                                f"No tasks available (attempt {consecutive_no_tasks}, cause={cause}), "
                                f"backing off to {current_poll_interval:.1f}s poll interval"
                            )
                            fetch_cooldown_until = time.monotonic() + current_poll_interval

                sleep_interval = self._loop_tick_interval(base_poll_interval)
                await asyncio.sleep(sleep_interval)
        except asyncio.CancelledError as cancel_exc:
            logging.error(f"Validator run CANCELLED! Traceback:", exc_info=True)
            import traceback

            logging.error(f"Cancel traceback: {''.join(traceback.format_stack())}")
            logging.info("Validator run cancelled; cleaning up.")
            raise
        except Exception as e:
            logging.error(f"Exception in validator run loop: {e}", exc_info=True)
            raise


def get_heartbeat_dependency():
    """Returns dependency that gets expected_key at request time, not at startup"""
    def _get_expected_key():
        # This is called during request, not at startup
        validator = getattr(app.state, "validator", None)
        if validator is None:
            raise HTTPException(status_code=503, detail="Validator not initialized")
        return validator.settings.platform_signer_ss58
    
    async def _dependency(
        request: Request,
        env: SignedEnvelope[dict],
        debug: bool = False,
    ) -> SignedEnvelope[HeartbeatRequest]:
        expected_key = _get_expected_key()  # Get key at request time
        return await verify_request_dep_no_db(
            HeartbeatRequest,
            expected_key=expected_key,
        )(request, env, debug)
    
    return _dependency

def create_app() -> FastAPI:
    configure_logging()
    app = FastAPI(title="MCP Validator")

    @app.on_event("startup")
    async def startup_event():
        validator = await asyncio.to_thread(Validator)
        app.state.validator = validator
        app.state.validator_task = asyncio.create_task(validator.run())

    @app.on_event("shutdown")
    async def shutdown_event():
        task = getattr(app.state, "validator_task", None)
        if task:
            task.cancel()
        validator = getattr(app.state, "validator", None)
        if validator is not None:
            client = getattr(validator, "client", None)
            if client is not None:
                try:
                    await client.aclose()
                    logging.info("HTTP client closed")
                except Exception:
                    logging.exception("Failed to close validator HTTP client")

    @app.post("/heartbeat", response_model=SignedEnvelope[HeartbeatResponse])
    async def heartbeat(
        request: SignedEnvelope[HeartbeatRequest] = Depends(get_heartbeat_dependency()),
    ) -> SignedEnvelope[HeartbeatResponse]:
        validator = getattr(app.state, "validator", None)
        if validator is None:
            logging.error("Validator is None, raising 503")
            raise HTTPException(status_code=503, detail="Validator not initialized")
        payload = HeartbeatResponse(ok=True, server_ts=datetime.now(timezone.utc))
        nonce = generate_nonce()
        signature = sign_payload_model(
            payload=payload, nonce=nonce, wallet=validator.settings.wallet
        )
        logging.info("Heartbeat response signed, returning to caller")
        return SignedEnvelope(
            payload=payload,
            sig=signature,
        )

    return app


app = create_app()
