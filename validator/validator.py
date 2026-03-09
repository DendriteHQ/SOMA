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
from validator.evaluation.evaluator import Evaluator
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
        self.evaluator = Evaluator(settings=self.settings)
        self.weight_setter = WeightSetter(
            netuid=self.settings.netuid, subtensor=self.settings.subtensor
        )
        self.client = None
        resp = asyncio.run(self.register_to_platform())
        self.registered = bool(resp and getattr(resp, "ok", False))
        if not self.registered:
            raise RuntimeError("Validator registration to platform failed.")
        

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
                    signed = verify_httpx_response(
                        response,
                        ValidatorRegisterResponse,
                        expected_key=self.settings.platform_signer_ss58,
                    )
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

    async def get_tasks_for_eval(self) -> GetChallengesResponse:
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
                        if "No tasks available" in error_detail:
                            logging.info(
                                "Platform reports no tasks available - all miners are scored. "
                                "Validator will retry later with backoff."
                            )
                            logging.info("get_tasks_for_eval: Returning None (503)")
                            return None
                    except Exception as e503:
                        logging.error(
                            f"get_tasks_for_eval: Error parsing 503 response: {e503}",
                            exc_info=True,
                        )
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
                logging.info("get_tasks_for_eval: Returning None (HTTP error)")
                return None
        except Exception as e:
            logging.error(f"Exception during get_tasks_for_eval: {e}", exc_info=True)
            logging.info("get_tasks_for_eval: Returning None (exception)")
            return None

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
                signed = verify_httpx_response(
                    response,
                    PostChallengeScoresResponse,
                    expected_key=self.settings.platform_signer_ss58,
                )
                logging.info(
                    "Successfully reported results",
                    extra={"payload": signed.payload.model_dump(mode="json")},
                )
            except httpx.HTTPError as e:
                logging.error(f"Failed to report results: {e}", exc_info=True)
        except Exception as e:
            logging.error(f"Exception during reporting results: {e}", exc_info=True)

    def has_eval_capacity(self) -> bool:
        return self.evaluator.has_eval_capacity()

    async def run(self) -> None:
        # Initialize async resources in the correct event loop
        await self.async_init()
        
        # TODO change this for a blocks from chain instead of mocked time intervals
        weight_interval_seconds = 360 * 12
        base_poll_interval = self.settings.task_poll_interval_seconds
        max_backoff_interval = self.settings.max_backoff_interval_seconds
        backoff_multiplier = self.settings.backoff_multiplier

        current_poll_interval = base_poll_interval
        consecutive_no_tasks = 0

        in_flight: set[asyncio.Task] = set()
        last_weight_set = 0.0
        weight_task: asyncio.Task | None = None

        async def process_task(task: GetChallengesResponse) -> None:
            try:
                results = await self.evaluator.evaluate(task)
                # logging.info(f"Async evaluation results: {results}")
                if results:
                    logging.info(f"Reporting results for task {task.batch_id}")
                    await self.report_results(task, results)

            except Exception as exc:
                logging.error(
                    f"Failed to process task {getattr(task, 'batch_id', 'unknown')}: {exc}",
                    exc_info=True,
                )

        try:
            logging.info("Validator run loop started")
            while True:
                now = time.monotonic()
                # TODO Change to blocks operation
                if (
                    weight_interval_seconds > 0
                    and (now - last_weight_set) >= weight_interval_seconds
                ):
                    if weight_task is None or weight_task.done():
                        logging.info("Creating weight setting task")
                        weight_task = asyncio.create_task(self.set_weights())
                        last_weight_set = now

                in_flight = {task for task in in_flight if not task.done()}
                has = self.has_eval_capacity()
                logging.info(
                    f"Has evaluation capacity: {has}, in_flight tasks: {len(in_flight)}"
                )
                if has:
                    logging.info("Fetching tasks from platform...")
                    task = await self.get_tasks_for_eval()
                    logging.info(f"Got task: {task}")
                    # logging.info(f"Fetched task: {task}")
                    if task and getattr(task, "challenges", None):
                        # Successfully got tasks - reset backoff
                        consecutive_no_tasks = 0
                        current_poll_interval = base_poll_interval
                        logging.info(
                            f"Fetched task: {task.batch_id}, reset poll interval to {current_poll_interval}s"
                        )
                        in_flight.add(asyncio.create_task(process_task(task)))
                    elif task is None:
                        # No tasks available (503 response) - apply backoff
                        consecutive_no_tasks += 1
                        current_poll_interval = min(
                            base_poll_interval
                            * (backoff_multiplier**consecutive_no_tasks),
                            max_backoff_interval,
                        )
                        logging.info(
                            f"No tasks available (attempt {consecutive_no_tasks}), "
                            f"backing off to {current_poll_interval:.1f}s poll interval"
                        )

                await asyncio.sleep(current_poll_interval)
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
        if os.getenv("VALIDATOR_DISABLE_RUN") == "1":
            logging.info("Validator run disabled via VALIDATOR_DISABLE_RUN=1")
            return
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
