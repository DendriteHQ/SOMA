from __future__ import annotations

import asyncio
from datetime import UTC, datetime
import json
import logging
import os
from pathlib import Path
import random
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


def _failed_calls_dir() -> Path:
    return Path(os.getenv("GATEWAY_FAILED_CALLS_DIR", "/root/SOMA/gateway/metadata/failed_calls"))


def _decode_body_preview(body: bytes, *, limit: int = 20000) -> str:
    if not body:
        return ""
    return body[:limit].decode("utf-8", errors="replace")


def _load_json_body(body: bytes) -> dict | list | None:
    if not body:
        return None
    try:
        payload = json.loads(body)
    except Exception:
        return None
    if isinstance(payload, (dict, list)):
        return payload
    return None


def _failed_call_record_path(now: datetime) -> Path:
    return _failed_calls_dir() / f"{now.date().isoformat()}.jsonl"


def _append_text_line(path: Path, line: str) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line)


async def _dump_failed_call(
    *,
    run_id: int,
    miner_hotkey: str,
    method: str,
    path: str,
    url: str,
    stream: bool,
    request_body: bytes,
    response_status_code: int | None,
    response_headers: httpx.Headers | dict[str, str] | None,
    response_body: bytes | None,
    attempts_used: int,
    retry_reason: str | None,
    forced_provider: str | None,
    actual_provider: str | None,
    exception_message: str | None = None,
) -> None:
    now = datetime.now(UTC)
    payload = {
        "timestamp": now.isoformat(),
        "run_id": run_id,
        "miner_hotkey": miner_hotkey,
        "method": method,
        "path": f"/v1/{path}",
        "url": url,
        "stream": stream,
        "status_code": response_status_code,
        "attempts_used": attempts_used,
        "max_attempts": _provider_retry_max_attempts(),
        "retry_reason": retry_reason,
        "forced_provider": forced_provider,
        "actual_provider": actual_provider,
        "request": _load_json_body(request_body) or _decode_body_preview(request_body),
        "response_headers": dict(response_headers.items()) if response_headers is not None else None,
        "response_json": _load_json_body(response_body or b""),
        "response_body_preview": _decode_body_preview(response_body or b""),
        "exception_message": exception_message,
    }
    output_path = _failed_call_record_path(now)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(payload, ensure_ascii=True) + "\n"
    await asyncio.to_thread(_append_text_line, output_path, line)


def _provider_retry_max_attempts() -> int:
    try:
        value = int(os.getenv("GATEWAY_PROVIDER_RETRY_MAX_ATTEMPTS", "3"))
    except ValueError:
        value = 3
    return max(1, value)


def _provider_retry_base_delay_seconds() -> float:
    try:
        value = float(os.getenv("GATEWAY_PROVIDER_RETRY_BASE_DELAY_SECONDS", "0.75"))
    except ValueError:
        value = 0.75
    return max(0.0, value)


def _provider_retry_max_delay_seconds() -> float:
    try:
        value = float(os.getenv("GATEWAY_PROVIDER_RETRY_MAX_DELAY_SECONDS", "3.0"))
    except ValueError:
        value = 3.0
    return max(0.0, value)


def _provider_retry_jitter_seconds() -> float:
    try:
        value = float(os.getenv("GATEWAY_PROVIDER_RETRY_JITTER_SECONDS", "0.35"))
    except ValueError:
        value = 0.35
    return max(0.0, value)


def _extract_response_error_message(body: bytes) -> str:
    if not body:
        return ""
    try:
        payload = json.loads(body)
    except Exception:
        return body.decode("utf-8", errors="replace")
    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict):
            message = error.get("message")
            if isinstance(message, str):
                return message
        message = payload.get("message")
        if isinstance(message, str):
            return message
    return body.decode("utf-8", errors="replace")


def _classify_retryable_provider_error(status_code: int, body: bytes) -> str | None:
    message = _extract_response_error_message(body)
    if status_code == 404 and (
        "not found on provider" in message
        or "No endpoints found for" in message
    ):
        return "provider_model_404"
    if status_code == 400:
        return "provider_400"
    return None


def _provider_retry_delay_seconds(attempt_no: int) -> float:
    base_delay = _provider_retry_base_delay_seconds()
    max_delay = _provider_retry_max_delay_seconds()
    jitter = _provider_retry_jitter_seconds()
    delay = min(max_delay, base_delay * (2 ** max(0, attempt_no - 1)))
    if jitter > 0:
        delay += random.uniform(0.0, jitter)
    return delay


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
        if lower in skip or lower.startswith("x-run-id"):
            continue
        headers[key] = value
    return headers


def _extract_run_id_from_authorization(request: Request) -> int | None:
    raw_auth = request.headers.get("authorization")
    if not isinstance(raw_auth, str) or not raw_auth.strip():
        return None
    parts = raw_auth.strip().split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    token = parts[1].strip()
    if not token:
        return None
    try:
        return int(token)
    except ValueError:
        return None


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "healthy", "service": "gateway"}


async def _resolve_authorization_header(run_id: int) -> tuple[str, str]:
    try:
        baseline_run, api_key_path, miner_hotkey = await _resolve_run_auth_context(run_id)
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
        logger.exception("Failed to resolve API key for run_id=%s", run_id)
        raise HTTPException(status_code=400, detail=f"API key resolution failed: {exc}") from exc
    return f"Bearer {api_key_value}", resolved_hotkey


async def _stream_upstream_response(
    *,
    run_id: int,
    miner_hotkey: str,
    method: str,
    url: str,
    headers: dict[str, str],
    body_bytes: bytes,
    timeout: httpx.Timeout,
) -> tuple[
    AsyncIterator[bytes] | None,
    int,
    dict[str, str],
    httpx.Headers | None,
    bytes | None,
    int,
    str | None,
]:
    max_attempts = _provider_retry_max_attempts()
    for attempt_no in range(1, max_attempts + 1):
        client = httpx.AsyncClient(timeout=timeout)
        stream_ctx = client.stream(method=method, url=url, headers=headers, content=body_bytes)
        try:
            response = await stream_ctx.__aenter__()
        except Exception:
            await client.aclose()
            raise

        retry_reason: str | None = None
        error_body: bytes | None = None
        if response.status_code >= 400:
            error_body = await response.aread()
            retry_reason = _classify_retryable_provider_error(response.status_code, error_body)

        if retry_reason is not None and attempt_no < max_attempts:
            await stream_ctx.__aexit__(None, None, None)
            await client.aclose()
            delay_seconds = _provider_retry_delay_seconds(attempt_no)
            logger.warning(
                "gateway_provider_retry run_id=%s miner_hotkey=%s attempt=%s max_attempts=%s status_code=%s reason=%s delay_seconds=%.3f stream=true",
                run_id,
                miner_hotkey,
                attempt_no,
                max_attempts,
                response.status_code,
                retry_reason,
                delay_seconds,
            )
            await asyncio.sleep(delay_seconds)
            continue

        if error_body is not None:
            response_headers = _select_passthrough_response_headers(response.headers)
            response_headers.setdefault(
                "content-type",
                response.headers.get("content-type", "application/json"),
            )
            await stream_ctx.__aexit__(None, None, None)
            await client.aclose()
            return (
                None,
                response.status_code,
                response_headers,
                response.headers,
                error_body,
                attempt_no,
                retry_reason,
            )

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
        return (
            _iterator(),
            response.status_code,
            response_headers,
            response.headers,
            None,
            attempt_no,
            None,
        )

    raise RuntimeError("provider retry loop exhausted unexpectedly")


def _extract_actual_provider(headers: httpx.Headers, body: bytes | None = None) -> str | None:
    provider = headers.get("x-openrouter-provider") or headers.get("x-provider")
    if provider:
        return provider.strip()
    if body:
        try:
            data = json.loads(body)
            model = data.get("model") or ""
            if "/" in model:
                return model.split("/")[0]
        except Exception:
            pass
    return None


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


async def _send_upstream_request_with_retries(
    *,
    run_id: int,
    miner_hotkey: str,
    method: str,
    url: str,
    headers: dict[str, str],
    body_bytes: bytes,
    timeout: httpx.Timeout,
) -> tuple[httpx.Response, int, str | None]:
    max_attempts = _provider_retry_max_attempts()
    last_response: httpx.Response | None = None
    for attempt_no in range(1, max_attempts + 1):
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.request(method, url, headers=headers, content=body_bytes)
        except Exception:
            logger.exception(
                "gateway_upstream_failed run_id=%s miner_hotkey=%s method=%s path=%s attempt=%s",
                run_id,
                miner_hotkey,
                method,
                url,
                attempt_no,
            )
            raise

        last_response = resp
        retry_reason = _classify_retryable_provider_error(resp.status_code, resp.content)
        if retry_reason is None or attempt_no >= max_attempts:
            return resp, attempt_no, retry_reason

        delay_seconds = _provider_retry_delay_seconds(attempt_no)
        logger.warning(
            "gateway_provider_retry run_id=%s miner_hotkey=%s attempt=%s max_attempts=%s status_code=%s reason=%s delay_seconds=%.3f stream=false",
            run_id,
            miner_hotkey,
            attempt_no,
            max_attempts,
            resp.status_code,
            retry_reason,
            delay_seconds,
        )
        await asyncio.sleep(delay_seconds)

    if last_response is None:
        raise RuntimeError("provider retry loop exhausted without response")
    return last_response, max_attempts, None


@app.api_route("/v1/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy_openai_compatible(
    path: str,
    request: Request,
    x_run_id: str | None = Header(default=None, alias="X-Run-Id"),
) -> Response:
    run_id: int | None = None
    if x_run_id:
        try:
            run_id = int(x_run_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="X-Run-Id must be an integer") from exc
    if run_id is None:
        run_id = _extract_run_id_from_authorization(request)
    if run_id is None:
        raise HTTPException(
            status_code=400,
            detail="Missing run identifier. Provide X-Run-Id header or Authorization: Bearer <run_id>",
        )

    method = request.method.upper()
    logger.info("gateway_request_received run_id=%s method=%s path=/v1/%s", run_id, method, path)

    headers = _extract_forward_headers(request)
    auth_header, miner_hotkey = await _resolve_authorization_header(run_id)
    headers["Authorization"] = auth_header

    body_bytes = await request.body()
    parsed = None
    if body_bytes:
        try:
            parsed = await request.json()
        except Exception:
            parsed = None

    force_provider = (os.getenv("GATEWAY_FORCE_PROVIDER") or "").strip()
    provider_was_forced = False
    if force_provider and isinstance(parsed, dict) and method in ("POST", "PUT", "PATCH"):
        if "provider" not in parsed:
            parsed["provider"] = {"order": [force_provider], "allow_fallbacks": False}
            body_bytes = json.dumps(parsed).encode()
            provider_was_forced = True
            logger.info(
                "gateway_provider_forced run_id=%s forced_provider=%s model=%s",
                run_id,
                force_provider,
                parsed.get("model", "unknown"),
            )
        else:
            logger.info(
                "gateway_provider_not_forced run_id=%s forced_provider=%s reason=provider_already_set existing_provider=%s",
                run_id,
                force_provider,
                parsed.get("provider"),
            )

    url = _build_upstream_url(path, request.url.query)
    timeout = httpx.Timeout(float(os.getenv("GATEWAY_UPSTREAM_TIMEOUT_SECONDS", "180")))

    if isinstance(parsed, dict) and bool(parsed.get("stream")):
        try:
            stream, status_code, response_headers, upstream_headers, error_body, attempts_used, retry_reason = await _stream_upstream_response(
                run_id=run_id,
                miner_hotkey=miner_hotkey,
                method=method,
                url=url,
                headers=headers,
                body_bytes=body_bytes,
                timeout=timeout,
            )
        except Exception as exc:
            logger.exception(
                "gateway_upstream_failed run_id=%s miner_hotkey=%s method=%s path=/v1/%s",
                run_id,
                miner_hotkey,
                method,
                path,
            )
            await _dump_failed_call(
                run_id=run_id,
                miner_hotkey=miner_hotkey,
                method=method,
                path=path,
                url=url,
                stream=True,
                request_body=body_bytes,
                response_status_code=502,
                response_headers=None,
                response_body=None,
                attempts_used=1,
                retry_reason=None,
                forced_provider=force_provider if provider_was_forced else None,
                actual_provider=None,
                exception_message=str(exc),
            )
            raise HTTPException(status_code=502, detail=f"Upstream request failed: {exc}") from exc
        if provider_was_forced:
            actual_provider = _extract_actual_provider(upstream_headers or httpx.Headers(), error_body)
            match = actual_provider.lower() == force_provider.lower() if actual_provider else None
            logger.info(
                "gateway_provider_verification run_id=%s forced_provider=%s actual_provider=%s match=%s stream=true",
                run_id,
                force_provider,
                actual_provider or "unknown",
                match,
            )
            if match is False:
                logger.warning(
                    "gateway_provider_mismatch run_id=%s forced_provider=%s actual_provider=%s stream=true",
                    run_id,
                    force_provider,
                    actual_provider,
                )
        else:
            actual_provider = _extract_actual_provider(upstream_headers or httpx.Headers(), error_body)
        if stream is None:
            await _dump_failed_call(
                run_id=run_id,
                miner_hotkey=miner_hotkey,
                method=method,
                path=path,
                url=url,
                stream=True,
                request_body=body_bytes,
                response_status_code=status_code,
                response_headers=upstream_headers,
                response_body=error_body,
                attempts_used=attempts_used,
                retry_reason=retry_reason,
                forced_provider=force_provider if provider_was_forced else None,
                actual_provider=actual_provider,
            )
            logger.info(
                "gateway_request_completed run_id=%s miner_hotkey=%s method=%s path=/v1/%s status_code=%s stream=true",
                run_id,
                miner_hotkey,
                method,
                path,
                status_code,
            )
            return Response(
                content=error_body or b"",
                status_code=status_code,
                media_type=response_headers.get("content-type"),
                headers=response_headers,
            )
        stream_response_headers = {
            key: value
            for key, value in response_headers.items()
            if key.lower() != "content-type"
        }
        logger.info(
            "gateway_request_completed run_id=%s miner_hotkey=%s method=%s path=/v1/%s status_code=%s stream=true",
            run_id,
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
        resp, attempts_used, retry_reason = await _send_upstream_request_with_retries(
            run_id=run_id,
            miner_hotkey=miner_hotkey,
            method=method,
            url=url,
            headers=headers,
            body_bytes=body_bytes,
            timeout=timeout,
        )
    except Exception as exc:
        logger.exception(
            "gateway_upstream_failed run_id=%s miner_hotkey=%s method=%s path=/v1/%s",
            run_id,
            miner_hotkey,
            method,
            path,
        )
        await _dump_failed_call(
            run_id=run_id,
            miner_hotkey=miner_hotkey,
            method=method,
            path=path,
            url=url,
            stream=False,
            request_body=body_bytes,
            response_status_code=502,
            response_headers=None,
            response_body=None,
            attempts_used=1,
            retry_reason=None,
            forced_provider=force_provider if provider_was_forced else None,
            actual_provider=None,
            exception_message=str(exc),
        )
        raise HTTPException(status_code=502, detail=f"Upstream request failed: {exc}") from exc

    passthrough_headers = _select_passthrough_response_headers(resp.headers)
    if provider_was_forced:
        actual_provider = _extract_actual_provider(resp.headers, resp.content)
        match = actual_provider.lower() == force_provider.lower() if actual_provider else None
        logger.info(
            "gateway_provider_verification run_id=%s forced_provider=%s actual_provider=%s match=%s",
            run_id,
            force_provider,
            actual_provider or "unknown",
            match,
        )
        if match is False:
            logger.warning(
                "gateway_provider_mismatch run_id=%s forced_provider=%s actual_provider=%s",
                run_id,
                force_provider,
                actual_provider,
            )
    else:
        actual_provider = _extract_actual_provider(resp.headers, resp.content)
    if resp.status_code >= 400:
        await _dump_failed_call(
            run_id=run_id,
            miner_hotkey=miner_hotkey,
            method=method,
            path=path,
            url=url,
            stream=False,
            request_body=body_bytes,
            response_status_code=resp.status_code,
            response_headers=resp.headers,
            response_body=resp.content,
            attempts_used=attempts_used,
            retry_reason=retry_reason,
            forced_provider=force_provider if provider_was_forced else None,
            actual_provider=actual_provider,
        )
    logger.info(
        "gateway_request_completed run_id=%s miner_hotkey=%s method=%s path=/v1/%s status_code=%s",
        run_id,
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
