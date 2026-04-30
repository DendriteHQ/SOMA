from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
import tiktoken

from fastapi import HTTPException, status, Request
from sqlalchemy.ext.asyncio import AsyncSession
import ipaddress
from soma_shared.db.models.miner import Miner
from soma_shared.db.models.script import Script
from soma_shared.db.models.validator import Validator
from soma_shared.db.models.request import Request as RequestModel
from soma_shared.db.validator_log import log_validator_message
from app.core.config import settings
from app.api.deps import get_script_storage
from app.core.logging import get_logger
from app.db.interfaces import fetch_top_screener_miner_ids_for_competition
from app.db.interfaces.burn_weight_queries import (
    get_latest_burn_request_row,
)
from app.db.interfaces.competition_queries import (
    get_active_competition_id_from_view,
    get_active_competition_phase_row,
)
from app.db.interfaces.miner_selection_queries import (
    count_screener_backlog_miners,
    get_miner_and_script_for_competition,
    select_competition_phase_miner_ss58,
    select_screener_phase_miner_ss58,
)
from app.db.interfaces.request_queries import (
    get_request_model_by_external_request_id,
)
from app.db.interfaces.validator_identity_queries import (
    get_validator_by_ss58_unarchived,
)

logger = get_logger(__name__)
TOKENIZER_CHEATING_CHARS_PER_TOKEN_THRESHOLD = 1.3


@lru_cache(maxsize=1)
def _get_nlp():
    return tiktoken.get_encoding("cl100k_base")


def _count_tokens(text: str) -> int:
    encoding = _get_nlp()
    return len(encoding.encode_ordinary(text))


def _chars_per_token(text: str) -> float:
    token_count = _count_tokens(text)
    if token_count <= 0:
        return 0.0
    return len(text) / token_count


def _is_chars_per_token_outlier(
    original: str,
    compressed: str,
    threshold: float = TOKENIZER_CHEATING_CHARS_PER_TOKEN_THRESHOLD,
) -> bool:
    original_chars_per_token = _chars_per_token(original)
    if original_chars_per_token <= 0:
        return False

    compressed_chars_per_token = _chars_per_token(compressed)
    chars_per_token_ratio = compressed_chars_per_token / original_chars_per_token
    return chars_per_token_ratio > threshold

def _extract_client_ip(request: Request) -> str | None:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        # X-Forwarded-For can contain multiple IPs; take the first hop.
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return None


def _is_trusted_proxy(request: Request) -> bool:
    client_host = request.client.host if request.client else None
    if not client_host:
        return False
    try:
        ip = ipaddress.ip_address(client_host)
    except ValueError:
        return False
    for cidr in settings.trusted_proxy_cidrs:
        try:
            if ip in ipaddress.ip_network(cidr, strict=False):
                return True
        except ValueError:
            continue
    return False


def _is_private_client_ip(client_ip: str | None) -> bool:
    if not client_ip:
        return False
    try:
        ip = ipaddress.ip_address(client_ip)
    except ValueError:
        return False
    for cidr in settings.private_network_cidrs:
        try:
            if ip in ipaddress.ip_network(cidr, strict=False):
                return True
        except ValueError:
            continue
    return False


async def _require_private_network(request: Request) -> None:
    if not _is_trusted_proxy(request):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Private network access only",
        )
    client_ip = _extract_client_ip(request)
    if not _is_private_client_ip(client_ip):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Private network access only",
        )

def _miner_status(
    competition_challenges: int | None,
    screener_challenges: int | None,
    pending_assignments_competition: int | None,
    pending_assignments_screener: int | None,
    scored_screened_challenges: int | None,
    scored_competition_challanges: int | None,
    is_in_top_screener: bool = False,
    has_script: bool = False,
    miner_banned_status: bool = False,
) -> str:
    """Determine miner status based on assigned challenges, scores, and role.

    Args:
        competition_challenges: Total number of active competition challenges
            for which this miner could receive scores, or None if unknown.
        screener_challenges: Total number of screener challenges assigned to
            this miner, or None if the miner has no screener role/assignments.
        pending_assignments_competition: Number of pending competition
            assignments (unscored competition challenges) for this miner, or
            None if not applicable.
        pending_assignments_screener: Number of pending screener assignments
            (unscored screener challenges) for this miner, or None if not
            applicable.
        scored_screened_challenges: Number of screener challenges that have
            been scored for this miner, or None if screener scoring does not
            apply.
        scored_competition_challanges: Number of competition challenges that
            have been scored for this miner, or None if competition scoring
            does not apply.
        is_in_top_screener: Whether this miner is in the top screener set for
            the current competition.
        has_script: Whether the miner has uploaded a script for the active
            competition.
        miner_banned_status: Whether the miner is currently banned from
            participating in the competition.

    Returns:
        One of:
            - 'banned': Miner is banned from participating.
            - 'idle': Miner has not uploaded a script.
            - 'scored': All competition challenges have been scored for this
              miner.
            - 'evaluating': The miner has competition challenges that are
              pending scoring.
            - 'screening': The miner is actively screening challenges (has
              pending or partially scored screener assignments).
            - 'qualified': The miner has completed screener challenges, is in
              the top screener set, and has no competition work in progress.
            - 'not qualified': The miner has completed screener challenges but
              is not in the top screener set.
            - 'in queue': Miner has uploaded a script but has no active
              competition or screener work in progress.
    """
    if miner_banned_status:
        return "banned"

    if not has_script:
        return "idle"

    if competition_challenges is not None and scored_competition_challanges is not None:
        if scored_competition_challanges >= competition_challenges:
            return "scored"
        elif (
            scored_competition_challanges > 0
            and scored_competition_challanges < competition_challenges
        ):
            return "evaluating"

    if pending_assignments_screener is not None and pending_assignments_screener > 0:
        return "screening"
    # Only check screener status if miner actually has screener challenges assigned
    if (
        screener_challenges is not None
        and screener_challenges > 0
        and scored_screened_challenges is not None
    ):
        if scored_screened_challenges < screener_challenges:
            return "screening"
        elif (
            scored_screened_challenges >= screener_challenges
            and is_in_top_screener
            and (
                pending_assignments_competition is None
                or pending_assignments_competition == 0
            )
            and (
                scored_competition_challanges is None
                or scored_competition_challanges == 0
            )
        ):
            return "qualified"
        elif (
            scored_screened_challenges >= screener_challenges and not is_in_top_screener
        ):
            return "not qualified"

    if (
        pending_assignments_competition is not None
        and pending_assignments_competition > 0
    ):
        return "evaluating"

    return "in queue"

def _is_compressed_enough(
    original: str,
    compressed: str,
    ratio: float | None,
) -> bool:
    if not compressed.strip():
        return False

    if _is_chars_per_token_outlier(original=original, compressed=compressed):
        return False

    if ratio is None:
        return True
    if ratio <= 0:
        return False

    original_tokens = _count_tokens(original)
    if original_tokens == 0:
        return False

    compressed_tokens = _count_tokens(compressed)
    return (compressed_tokens / original_tokens) <= ratio


async def _log_error_response(
    request: Request,
    db: AsyncSession,
    status_code: int,
    detail: str,
    *,
    exc: Exception | None = None,
) -> None:
    request_id = getattr(request.state, "request_id", None)
    log_extra = {
        "request_id": request_id,
        "endpoint": request.url.path,
        "method": request.method,
        "status_code": status_code,
        "detail": detail,
    }
    if exc is not None:
        logger.warning(
            "validator_error_response",
            extra=log_extra,
            exc_info=exc,
        )
    else:
        logger.warning(
            "validator_error_response",
            extra=log_extra,
        )
    await log_validator_message(
        db,
        direction="response",
        endpoint=request.url.path,
        method=request.method,
        signature=None,
        nonce=None,
        request_id=request_id,
        payload={"detail": detail},
        status_code=status_code,
    )


async def _get_active_competition_id(db: AsyncSession) -> int | None:
    return await get_active_competition_id_from_view(db)


async def _get_current_burn_state(db: AsyncSession) -> tuple[bool, float]:
    default_ratio = 1.0
    default_active_no_row = False if settings.debug else True
    default_active_on_error = False
    try:
        latest_burn = await get_latest_burn_request_row(db)
    except Exception as exc:
        if db.in_transaction():
            await db.rollback()
        logger.warning(
            "burn_state_load_failed",
            extra={"error": str(exc)},
            exc_info=exc,
        )
        return default_active_on_error, default_ratio

    if latest_burn is None:
        return default_active_no_row, default_ratio

    burn_ratio = max(0.0, min(1.0, float(latest_burn.burn_ratio)))
    return bool(latest_burn.is_active), burn_ratio


async def _select_miner_ss58(
    request: Request,
    db: AsyncSession,
) -> tuple[Miner, Script]:
    """
    Select script by earliest upload time in the active competition (FIFO).
    Upload phase: only screening challenges are scored.
    Evaluation phase: first finish screener backlog, then assign competition 
    challenges to top screeners only.
    
    Returns:
        (Miner, Script): miner + selected script, or (None, None) if no work available
    """
    logger.info("_select_miner_ss58: Starting miner selection")

    comp_row = await get_active_competition_phase_row(db)
    if not comp_row:
        logger.info("_select_miner_ss58: No active competition found")
        return None, None

    competition_id = int(comp_row.competition_id)
    eval_starts_at = comp_row.eval_starts_at

    now = datetime.now(timezone.utc)
    if eval_starts_at and eval_starts_at.tzinfo is None:
        eval_starts_at = eval_starts_at.replace(tzinfo=timezone.utc)
    is_eval_phase = eval_starts_at and now >= eval_starts_at

    miner_ss58 = None

    if not is_eval_phase:
        # UPLOAD PHASE: only screener work
        logger.info("_select_miner_ss58: Upload phase - assigning screener work")
        miner_ss58 = await select_screener_phase_miner_ss58(
            db,
            competition_id=competition_id,
        )
    else:
        # EVAL PHASE: first screener backlog, then competition work

        # Check if screener backlog exists
        screener_backlog = await count_screener_backlog_miners(
            db,
            competition_id=competition_id,
        )

        if screener_backlog and screener_backlog > 0:
            # Still have screener backlog - continue screener work
            logger.info(
                f"_select_miner_ss58: Eval phase - screener backlog exists "
                f"({screener_backlog} miners), continuing screener work"
            )
            miner_ss58 = await select_screener_phase_miner_ss58(
                db,
                competition_id=competition_id,
            )
        else:
            # Screener done - assign competition work to top screeners
            logger.info(
                "_select_miner_ss58: Eval phase - screener complete, "
                "assigning competition work to top screeners"
            )
            
            top_fraction = float(getattr(settings, "top_screener_scripts", 0.2))
            if top_fraction <= 0:
                logger.info("_select_miner_ss58: Top screener fraction is 0")
                return None, None

            top_miner_ids, total_eligible, top_limit = await fetch_top_screener_miner_ids_for_competition(
                db,
                competition_id=competition_id,
                top_screener_scripts=top_fraction,
            )

            if total_eligible <= 0:
                logger.info("_select_miner_ss58: No eligible screeners found")
                return None, None
            if top_limit <= 0:
                logger.info("_select_miner_ss58: Top limit is 0")
                return None, None
            if not top_miner_ids:
                logger.info("_select_miner_ss58: No top screener miners found")
                return None, None

            logger.info(
                f"_select_miner_ss58: Top screener limit: {top_limit} "
                f"(fraction={top_fraction}, total_eligible={total_eligible})"
            )
            logger.info(f"_select_miner_ss58: Found {len(top_miner_ids)} top screeners")

            # Find top screener with free competition work
            miner_ss58 = await select_competition_phase_miner_ss58(
                db,
                competition_id=competition_id,
                top_miner_ids=top_miner_ids,
            )

    if not miner_ss58:
        logger.info("_select_miner_ss58: No miners with free work found")
        return None, None

    miner_script_pair = await get_miner_and_script_for_competition(
        db,
        miner_ss58=miner_ss58,
        competition_id=competition_id,
    )
    if not miner_script_pair:
        logger.warning(
            f"_select_miner_ss58: Found miner_ss58={miner_ss58} in candidate miner selection "
            "but could not retrieve Miner/Script objects"
        )
        return None, None

    miner, script = miner_script_pair

    logger.info(
        f"_select_miner_ss58: Selected miner_ss58={miner.ss58}, "
        f"script_id={script.id}, script_uuid={script.script_uuid}"
    )

    return miner, script


def get_script_s3_key(miner_ss58: str, script: Script) -> str:
    """
    Return the S3 key for the miner's challenge script without fetching it.
    In DEBUG mode returns the debug prefix key; otherwise the hot prefix key.
    """
    from app.core.config import settings

    #if settings.debug:
    #    return f"debug/miner_solutions/{miner_ss58}/{script.script_uuid}.py"

    date_prefix = (
        script.created_at.strftime("%Y-%m-%d") if script.created_at else None
    )
    script_storage = get_script_storage()
    return script_storage.hot_key(
        miner_ss58=miner_ss58,
        script_uuid=script.script_uuid,
        date_prefix=date_prefix,
    )



async def _get_request_row(
    db: AsyncSession,
    *,
    request_id: str | None,
    endpoint: str,
    method: str,
    payload: dict,
) -> RequestModel | None:
    if not request_id:
        return None
    request_row = await get_request_model_by_external_request_id(
        db,
        request_id=request_id,
    )
    if request_row is None:
        request_row = RequestModel(
            external_request_id=request_id,
            endpoint=endpoint,
            method=method,
            payload=payload,
        )
        db.add(request_row)
        await db.flush()
    return request_row


async def _get_validator(
    db: AsyncSession,
    *,
    ss58: str,
) -> Validator:
    """
    Get existing validator by ss58 address.
    Raises HTTPException if validator is not found or archived.
    """
    validator = await get_validator_by_ss58_unarchived(
        db,
        ss58=ss58,
    )
    if validator is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Validator with ss58={ss58} not found or archived. "
                "Please register first."
            ),
        )

    # Update last_seen_at
    validator.last_seen_at = datetime.now(timezone.utc)
    return validator
