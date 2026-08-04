from __future__ import annotations

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from fastapi import APIRouter, Depends, Request

from soma_shared.contracts.sandbox.v1.messages import (
    CompactBenchReportRequest,
    CompactBenchReportResponse,
)
from soma_shared.db.models import SweBenchRun
from soma_shared.db.session import get_db_session

from app.api.routes.utils import _require_private_network
from app.core.logging import get_logger
from app.services.blob.patch_artifact_storage import PatchArtifactStorage
from app.services.blob.s3 import S3BlobStorage


logger = get_logger(__name__)


router = APIRouter(
    prefix="/api/private/sandbox",
    tags=["sandbox"],
)


def _model_attr(model: type, name: str):
    try:
        return getattr(model, name)
    except AttributeError:
        return None


def _get_s3_storage(app) -> S3BlobStorage:
    s3_storage = getattr(app.state, "swebench_s3_storage", None)
    if s3_storage is None:
        s3_storage = S3BlobStorage()
        app.state.swebench_s3_storage = s3_storage
    return s3_storage


def _get_output_storage(app) -> PatchArtifactStorage:
    output_storage = getattr(app.state, "swebench_output_storage", None)
    if output_storage is None:
        output_storage = PatchArtifactStorage(_get_s3_storage(app))
        app.state.swebench_output_storage = output_storage
    return output_storage


def _extract_compact_bench_error(payload: CompactBenchReportRequest) -> str | None:
    if payload.error and payload.error.strip():
        return payload.error.strip()

    metadata = payload.metadata or {}
    if not isinstance(metadata, dict):
        metadata = {}

    candidates = [
        "error",
        "task_error",
        "runtime_error",
        "failure_reason",
        "reason",
        "exception",
        "stderr",
    ]
    for key in candidates:
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    result = metadata.get("result")
    if isinstance(result, dict):
        for key in candidates:
            value = result.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    return None


def _coerce_optional_non_negative_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float) and value.is_integer() and value >= 0:
        return int(value)
    return None


def _request_client_host(request: Request) -> str | None:
    client = getattr(request, "client", None)
    if client is None:
        return None
    return getattr(client, "host", None)


async def _persist_compact_bench_report(
    db: AsyncSession,
    *,
    payload: CompactBenchReportRequest,
    request: Request,
) -> bool:
    run = (
        await db.execute(
            select(SweBenchRun).where(SweBenchRun.id == payload.run_id)
        )
    ).scalar_one_or_none()
    if run is None:
        logger.warning(
            "compact_bench_report_run_missing",
            extra={
                "run_id": payload.run_id,
            },
        )
        # Acknowledge orphaned callbacks to avoid endless retries in sandbox
        # after data resets / test cleanups on platform side.
        return True

    logger.info(
        "compact_bench_report_received",
        extra={
            "run_id": payload.run_id,
            "client_host": _request_client_host(request),
            "request_path": str(request.url.path),
            "ok_status": payload.ok_status,
            "patch_capture_status": payload.patch_capture_status,
            "trajectory_upload_status": payload.trajectory_upload_status,
            "compression_logs_upload_status": payload.compression_logs_upload_status,
            "prior_trajectory_uuid": getattr(run, "trajectory_uuid", None),
            "prior_compression_logs_uuid": getattr(run, "compression_logs_uuid", None),
            "payload_metadata": payload.metadata if isinstance(payload.metadata, dict) else None,
        },
    )

    patch_save_error: str | None = None
    patch_saved = False
    if payload.patch_diff is not None:
        try:
            output_storage = _get_output_storage(request.app)
            await output_storage.save_single(run.diff_storage_uuid, payload.patch_diff)
            patch_saved = True
        except Exception as exc:
            patch_save_error = str(exc)
            logger.exception(
                "compact_bench_report_patch_save_failed",
                extra={
                    "run_id": payload.run_id,
                    "storage_uuid": run.diff_storage_uuid,
                    "request_path": str(request.url.path),
                },
            )

    input_tokens = _coerce_optional_non_negative_int(getattr(payload, "input_tokens", None))
    cached_input_tokens = _coerce_optional_non_negative_int(
        getattr(payload, "cached_input_tokens", None)
    )
    output_tokens = _coerce_optional_non_negative_int(getattr(payload, "output_tokens", None))
    resolved_total_tokens = _coerce_optional_non_negative_int(payload.total_tokens)
    if resolved_total_tokens is None:
        split_values = [input_tokens, cached_input_tokens, output_tokens]
        if any(value is not None for value in split_values):
            resolved_total_tokens = sum(value for value in split_values if value is not None)

    run.agent_steps = payload.agent_steps
    run.tokens_used = resolved_total_tokens
    for field_name, value in (
        ("input_tokens", input_tokens),
        ("cached_input_tokens", cached_input_tokens),
        ("output_tokens", output_tokens),
    ):
        if _model_attr(SweBenchRun, field_name) is not None:
            setattr(run, field_name, value)
    run.time_taken_seconds = payload.execution_time_seconds
    if any(
        _model_attr(SweBenchRun, field_name) is None
        for field_name in ("input_tokens", "cached_input_tokens", "output_tokens")
    ) and any(value is not None for value in (input_tokens, cached_input_tokens, output_tokens)):
        # Backward compatibility when ORM model doesn't expose split token fields,
        # but the DB table already includes these columns.
        try:
            await db.execute(
                text(
                    """
                    UPDATE swe_bench_runs
                    SET input_tokens = :input_tokens,
                        cached_input_tokens = :cached_input_tokens,
                        output_tokens = :output_tokens,
                        updated_at = now()
                    WHERE id = :run_id
                    """
                ),
                {
                    "input_tokens": input_tokens,
                    "cached_input_tokens": cached_input_tokens,
                    "output_tokens": output_tokens,
                    "run_id": int(payload.run_id),
                },
            )
        except Exception:
            logger.exception(
                "compact_bench_report_split_token_persist_failed",
                extra={
                    "run_id": payload.run_id,
                    "input_tokens": input_tokens,
                    "cached_input_tokens": cached_input_tokens,
                    "output_tokens": output_tokens,
                },
            )
    # Keep the artifact UUID columns as references to objects that actually exist in
    # S3: clear them when the sandbox did not confirm the upload (failed upload, older
    # sandbox contract, or dispatch without a presigned URL).
    trajectory_uploaded = payload.trajectory_upload_status is True
    if _model_attr(SweBenchRun, "trajectory_uuid") is not None and not trajectory_uploaded:
        if run.trajectory_uuid is not None:
            logger.warning(
                "compact_bench_report_trajectory_uuid_cleared",
                extra={
                    "run_id": payload.run_id,
                    "cleared_uuid": run.trajectory_uuid,
                    "trajectory_upload_status": payload.trajectory_upload_status,
                },
            )
        run.trajectory_uuid = None
    compression_logs_uploaded = payload.compression_logs_upload_status is True
    if _model_attr(SweBenchRun, "compression_logs_uuid") is not None and not compression_logs_uploaded:
        if run.compression_logs_uuid is not None:
            logger.warning(
                "compact_bench_report_compression_logs_uuid_cleared",
                extra={
                    "run_id": payload.run_id,
                    "cleared_uuid": run.compression_logs_uuid,
                    "compression_logs_upload_status": payload.compression_logs_upload_status,
                },
            )
        run.compression_logs_uuid = None

    extracted_error = _extract_compact_bench_error(payload)
    desired_status = "completed" if (payload.ok_status and payload.patch_capture_status) else "failed"
    if patch_save_error:
        if extracted_error:
            resolved_last_error = f"{extracted_error}; patch save failed: {patch_save_error}"
        else:
            resolved_last_error = f"patch save failed: {patch_save_error}"
    elif desired_status == "failed":
        resolved_last_error = (
            extracted_error
            or (
                "Sandbox reported run as failed "
                f"(ok_status={payload.ok_status}, patch_capture_status={payload.patch_capture_status})."
            )
        )
    else:
        resolved_last_error = extracted_error

    last_error_col = _model_attr(SweBenchRun, "last_error")
    if last_error_col is not None:
        run.last_error = resolved_last_error
    else:
        # Backward compatibility when ORM model doesn't expose `last_error`,
        # but the DB table still has this column.
        await db.execute(
            text("UPDATE swe_bench_runs SET last_error = :last_error, updated_at = now() WHERE id = :run_id"),
            {"last_error": resolved_last_error, "run_id": int(payload.run_id)},
        )

    # Compatible with both old/new soma_shared branches.
    status_col = _model_attr(SweBenchRun, "status")
    if status_col is not None:
        run.status = desired_status
    else:
        # Backward compatibility when ORM model doesn't expose `status`,
        # but the DB table still has this column.
        await db.execute(
            text("UPDATE swe_bench_runs SET status = :status, updated_at = now() WHERE id = :run_id"),
            {"status": desired_status, "run_id": int(payload.run_id)},
        )

    await db.commit()
    logger.info(
        "compact_bench_report_persisted",
        extra={
            "run_id": payload.run_id,
            "client_host": _request_client_host(request),
            "request_path": str(request.url.path),
            "ok_status": payload.ok_status,
            "patch_capture_status": payload.patch_capture_status,
            "patch_saved": patch_saved,
            "trajectory_uploaded": trajectory_uploaded,
            "compression_logs_uploaded": compression_logs_uploaded,
            "status": desired_status,
            "tokens_used": resolved_total_tokens,
            "input_tokens": input_tokens,
            "cached_input_tokens": cached_input_tokens,
            "output_tokens": output_tokens,
            "has_error": bool(resolved_last_error),
            "error_excerpt": (resolved_last_error or "")[:240],
        },
    )
    return True


@router.post(
    "/compact-bench/report",
    response_model=CompactBenchReportResponse,
)
async def report_compact_bench_result(
    payload: CompactBenchReportRequest,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
    _: None = Depends(_require_private_network),
) -> CompactBenchReportResponse:
    persisted = False
    try:
        persisted = await _persist_compact_bench_report(db, payload=payload, request=request)
    except Exception:
        await db.rollback()
        logger.exception(
            "compact_bench_report_db_write_failed",
            extra={
                "run_id": payload.run_id,
                "path": str(request.url.path),
            },
        )
    return CompactBenchReportResponse(success=persisted)
