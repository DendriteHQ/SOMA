from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
from typing import AsyncIterator
from urllib.parse import quote

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import Response, StreamingResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from gateway.services.ssm.client import get_ssm_client


logger = logging.getLogger("soma.gateway")
logging.basicConfig(
    level=os.getenv("GATEWAY_LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


app = FastAPI(
    title="SOMA Gateway",
    description="Gateway service that proxies OpenAI-compatible requests",
    version="1.0.0",
)


def _resolve_postgres_dsn() -> str:
    dsn = os.getenv("POSTGRES_DSN")
    if dsn:
        if dsn.startswith("postgresql+asyncpg://"):
            return dsn
        if dsn.startswith("postgresql://"):
            return dsn.replace("postgresql://", "postgresql+asyncpg://", 1)
        if dsn.startswith("postgres://"):
            return dsn.replace("postgres://", "postgresql+asyncpg://", 1)
        return dsn

    secret_id = (os.getenv("RDS_SECRET_ID") or "").strip()
    if not secret_id:
        raise RuntimeError(
            "DB config missing: set POSTGRES_DSN or RDS_SECRET_ID (+ RDS settings) for gateway DB lookups",
        )

    cmd = [
        "aws",
        "secretsmanager",
        "get-secret-value",
        "--secret-id",
        secret_id,
        "--query",
        "SecretString",
        "--output",
        "text",
    ]
    result = subprocess.run(
        cmd,
        check=True,
        capture_output=True,
        text=True,
    )
    secret_string = result.stdout.strip()
    if not secret_string:
        raise RuntimeError("RDS secret has empty SecretString")
    try:
        secret = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise RuntimeError("RDS secret is not valid JSON") from exc

    use_reader = (os.getenv("RDS_USE_READER") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    writer_host = (os.getenv("RDS_WRITER_HOST") or "").strip()
    reader_host = (os.getenv("RDS_READER_HOST") or "").strip()
    host = reader_host if use_reader and reader_host else writer_host
    if not host:
        host = str(secret.get("host") or secret.get("hostname") or "").strip()
    if not host:
        raise RuntimeError("RDS host is missing (RDS_WRITER_HOST/RDS_READER_HOST or secret host)")

    user = str(secret.get("username") or "").strip()
    password = str(secret.get("password") or "").strip()
    if not user or not password:
        raise RuntimeError("RDS secret is missing username or password")

    db_name = (
        (os.getenv("RDS_DB_NAME") or "").strip()
        or str(secret.get("dbname") or secret.get("db_name") or secret.get("database") or "").strip()
    )
    if not db_name:
        raise RuntimeError("RDS database name is missing (RDS_DB_NAME or secret)")

    raw_port = (os.getenv("RDS_PORT") or "").strip() or str(secret.get("port") or "5432").strip()
    try:
        port = int(raw_port)
    except ValueError as exc:
        raise RuntimeError(f"Invalid RDS port: {raw_port!r}") from exc

    return (
        f"postgresql+asyncpg://{quote(user)}:{quote(password)}"
        f"@{host}:{port}/{quote(db_name)}"
    )


@app.on_event("startup")
async def startup() -> None:
    app.state.db_engine = create_async_engine(_resolve_postgres_dsn(), future=True)


@app.on_event("shutdown")
async def shutdown() -> None:
    engine: AsyncEngine | None = getattr(app.state, "db_engine", None)
    if engine is not None:
        await engine.dispose()


def _resolve_ssm_parameter_name(api_key_path: str) -> str:
    prefix = os.getenv("OPENROUTER_SSM_PREFIX", "/s114/dev")
    clean_prefix = prefix.rstrip("/")
    clean_suffix = api_key_path.strip("/")
    if not clean_suffix:
        raise ValueError("api_key_path must not be empty")
    return f"{clean_prefix}/{clean_suffix}"


def _resolve_api_key_from_ssm(api_key_path: str) -> str:
    parameter_name = _resolve_ssm_parameter_name(api_key_path)
    client = get_ssm_client()
    resp = client.get_parameter(Name=parameter_name, WithDecryption=True)
    value = (resp.get("Parameter") or {}).get("Value")
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"SSM parameter {parameter_name!r} is empty or missing")
    return value


def _resolve_upstream_url(path: str) -> str:
    base_url = os.getenv("GATEWAY_UPSTREAM_BASE_URL", "https://openrouter.ai/api/v1")
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


async def _resolve_run_auth_context(run_id: int) -> tuple[bool, str | None, str | None]:
    engine: AsyncEngine = app.state.db_engine
    query = text(
        """
        SELECT sbr.baseline_run, mok.secret_ref, m.ss58
        FROM swe_bench_runs sbr
        LEFT JOIN miner_openrouter_api_keys mok
            ON mok.miner_fk = sbr.miner_fk
           AND mok.revoked_at IS NULL
        LEFT JOIN miners m
            ON m.id = sbr.miner_fk
        WHERE sbr.id = :run_id
        LIMIT 1
        """
    )
    async with engine.connect() as conn:
        result = await conn.execute(query, {"run_id": run_id})
        row = result.first()
    if row is None:
        raise ValueError(f"swe_bench_runs.id={run_id} not found")
    baseline_run = bool(row[0])
    api_key_path = str(row[1]).strip() if row[1] is not None else None
    miner_hotkey = str(row[2]).strip() if row[2] is not None else None
    if not baseline_run and (not api_key_path):
        raise ValueError(
            f"No active miner OpenRouter key path found for swe_bench_runs.id={run_id}",
        )
    return baseline_run, api_key_path, miner_hotkey


def _extract_forward_headers(request: Request) -> dict[str, str]:
    skip = {"host", "content-length", "authorization"}
    headers: dict[str, str] = {}
    for key, value in request.headers.items():
        lower = key.lower()
        if lower in skip or lower.startswith("x-run-id") or lower.startswith("x-batch-id"):
            continue
        headers[key] = value
    return headers


async def _resolve_batch_auth_context(batch_id: int) -> tuple[bool, str | None, str | None]:
    engine: AsyncEngine = app.state.db_engine
    query = text(
        """
        SELECT
            CASE WHEN sbb.miner_fk IS NULL THEN TRUE ELSE FALSE END AS baseline_run,
            mok.secret_ref,
            m.ss58
        FROM swe_bench_run_batches sbb
        LEFT JOIN miner_openrouter_api_keys mok
            ON mok.miner_fk = sbb.miner_fk
           AND mok.revoked_at IS NULL
        LEFT JOIN miners m
            ON m.id = sbb.miner_fk
        WHERE sbb.id = :batch_id
        LIMIT 1
        """
    )
    async with engine.connect() as conn:
        result = await conn.execute(query, {"batch_id": batch_id})
        row = result.first()
    if row is None:
        raise ValueError(f"swe_bench_run_batches.id={batch_id} not found")
    baseline_run = bool(row[0])
    api_key_path = str(row[1]).strip() if row[1] is not None else None
    miner_hotkey = str(row[2]).strip() if row[2] is not None else None
    if not baseline_run and (not api_key_path):
        raise ValueError(
            f"No active miner OpenRouter key path found for swe_bench_run_batches.id={batch_id}",
        )
    return baseline_run, api_key_path, miner_hotkey


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "healthy", "service": "gateway"}


async def _resolve_authorization_header(*, run_id: int | None, batch_id: int | None) -> tuple[str, str]:
    try:
        if batch_id is not None:
            baseline_run, api_key_path, miner_hotkey = await _resolve_batch_auth_context(batch_id)
        elif run_id is not None:
            baseline_run, api_key_path, miner_hotkey = await _resolve_run_auth_context(run_id)
        else:
            raise ValueError("Either run_id or batch_id must be provided")
        if baseline_run:
            baseline_api_key = (os.getenv("GATEWAY_BASELINE_OPENROUTER_API_KEY") or "").strip()
            if not baseline_api_key:
                raise ValueError(
                    "GATEWAY_BASELINE_OPENROUTER_API_KEY must be set for baseline runs",
                )
            api_key_value = baseline_api_key
            resolved_hotkey = "baseline"
        else:
            api_key_value = await asyncio.to_thread(_resolve_api_key_from_ssm, str(api_key_path))
            resolved_hotkey = miner_hotkey or "unknown"
    except Exception as exc:
        logger.exception("Failed to resolve API key for run_id=%s batch_id=%s", run_id, batch_id)
        raise HTTPException(status_code=400, detail=f"API key resolution failed: {exc}") from exc
    return f"Bearer {api_key_value}", resolved_hotkey


async def _stream_upstream_response(
    method: str,
    url: str,
    headers: dict[str, str],
    body_bytes: bytes,
    timeout: httpx.Timeout,
) -> tuple[AsyncIterator[bytes], int, dict[str, str]]:
    client = httpx.AsyncClient(timeout=timeout)
    stream_ctx = client.stream(method=method, url=url, headers=headers, content=body_bytes)
    response = await stream_ctx.__aenter__()

    async def _iterator() -> AsyncIterator[bytes]:
        try:
            async for chunk in response.aiter_bytes():
                yield chunk
        finally:
            await stream_ctx.__aexit__(None, None, None)
            await client.aclose()

    response_headers = _select_passthrough_response_headers(response.headers)
    response_headers.setdefault(
        "content-type",
        response.headers.get("content-type", "text/event-stream"),
    )
    return _iterator(), response.status_code, response_headers


def _select_passthrough_response_headers(headers: httpx.Headers) -> dict[str, str]:
    allow = {
        "x-request-id",
        "openai-processing-ms",
        "x-ratelimit-limit-requests",
        "x-ratelimit-remaining-requests",
        "x-ratelimit-reset-requests",
        "x-ratelimit-limit-tokens",
        "x-ratelimit-remaining-tokens",
        "x-ratelimit-reset-tokens",
    }
    out: dict[str, str] = {}
    for key in allow:
        value = headers.get(key)
        if value:
            out[key] = value
    return out


def _build_upstream_url(path: str, query_string: str) -> str:
    base = _resolve_upstream_url(path)
    if query_string:
        return f"{base}?{query_string}"
    return base


@app.api_route("/v1/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy_openai_compatible(
    path: str,
    request: Request,
    x_run_id: str | None = Header(default=None, alias="X-Run-Id"),
    x_batch_id: str | None = Header(default=None, alias="X-Batch-Id"),
) -> Response:
    if not x_run_id and not x_batch_id:
        raise HTTPException(status_code=400, detail="Missing required header: X-Run-Id or X-Batch-Id")

    run_id: int | None = None
    batch_id: int | None = None
    if x_batch_id:
        try:
            batch_id = int(x_batch_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="X-Batch-Id must be an integer") from exc
    if x_run_id:
        try:
            run_id = int(x_run_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="X-Run-Id must be an integer") from exc

    method = request.method.upper()
    logger.info(
        "gateway_request_received run_id=%s batch_id=%s method=%s path=/v1/%s",
        run_id,
        batch_id,
        method,
        path,
    )

    headers = _extract_forward_headers(request)
    auth_header, miner_hotkey = await _resolve_authorization_header(run_id=run_id, batch_id=batch_id)
    headers["Authorization"] = auth_header

    body_bytes = await request.body()
    parsed = None
    if body_bytes:
        try:
            parsed = await request.json()
        except Exception:
            parsed = None

    url = _build_upstream_url(path, request.url.query)
    timeout = httpx.Timeout(float(os.getenv("GATEWAY_UPSTREAM_TIMEOUT_SECONDS", "180")))

    if isinstance(parsed, dict) and bool(parsed.get("stream")):
        stream, status_code, response_headers = await _stream_upstream_response(
            method=method,
            url=url,
            headers=headers,
            body_bytes=body_bytes,
            timeout=timeout,
        )
        stream_response_headers = {
            key: value
            for key, value in response_headers.items()
            if key.lower() != "content-type"
        }
        logger.info(
            "gateway_request_completed run_id=%s batch_id=%s miner_hotkey=%s method=%s path=/v1/%s status_code=%s stream=true",
            run_id,
            batch_id,
            miner_hotkey,
            method,
            path,
            status_code,
        )
        return StreamingResponse(
            stream,
            status_code=status_code,
            media_type=response_headers.get("content-type", "text/event-stream"),
            headers=stream_response_headers,
        )

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.request(method, url, headers=headers, content=body_bytes)
    except Exception as exc:
        logger.exception(
            "gateway_upstream_failed run_id=%s batch_id=%s miner_hotkey=%s method=%s path=/v1/%s",
            run_id,
            batch_id,
            miner_hotkey,
            method,
            path,
        )
        raise HTTPException(status_code=502, detail=f"Upstream request failed: {exc}") from exc

    passthrough_headers = _select_passthrough_response_headers(resp.headers)
    logger.info(
        "gateway_request_completed run_id=%s batch_id=%s miner_hotkey=%s method=%s path=/v1/%s status_code=%s",
        run_id,
        batch_id,
        miner_hotkey,
        method,
        path,
        resp.status_code,
    )
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        media_type=resp.headers.get("content-type"),
        headers=passthrough_headers,
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "gateway.main:app",
        host=os.getenv("GATEWAY_HOST", "0.0.0.0"),
        port=int(os.getenv("GATEWAY_PORT", "8010")),
        log_level=os.getenv("GATEWAY_LOG_LEVEL", "info").lower(),
    )
