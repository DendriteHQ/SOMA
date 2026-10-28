import json

import httpx
import pytest

from gateway import main as gateway


@pytest.fixture
def gateway_client(monkeypatch):
    monkeypatch.setenv("GATEWAY_FORCE_PROVIDER", "openai")

    async def fake_auth(_run_id):
        return "Bearer test", "test-hotkey"

    monkeypatch.setattr(gateway, "_resolve_authorization_header", fake_auth)
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gateway.app),
        base_url="http://gateway",
    )


def _provider_error_body() -> bytes:
    return json.dumps(
        {
            "error": {
                "message": "Sorry, something went wrong.",
                "code": 502,
                "metadata": {"error_type": "provider_unavailable"},
            },
            "choices": [],
            "object": "chat.completion.chunk",
        }
    ).encode()


def test_provider_unavailable_is_retryable_from_sse_event():
    body = b"data: " + _provider_error_body() + b"\n\n"

    assert gateway._is_provider_unavailable_error(body) is True
    assert gateway._classify_retryable_provider_error(200, body) == "provider_502"


def test_provider_unavailable_response_is_openai_compatible():
    response = json.loads(gateway._provider_unavailable_response(_provider_error_body()))

    assert response["error"]["code"] == "provider_unavailable"
    assert "HTTP 502" in response["error"]["message"]


@pytest.mark.asyncio
async def test_initial_sse_provider_error_is_detected_before_forwarding():
    async def handler(_request):
        return httpx.Response(200, content=b"data: " + _provider_error_body() + b"\n\n")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        async with client.stream("POST", "https://upstream.test/v1/chat/completions") as response:
            prefix, _iterator, is_provider_error = await gateway._read_initial_stream_event(response)

    assert prefix.startswith(b"data: ")
    assert is_provider_error is True


@pytest.mark.asyncio
async def test_non_stream_provider_5xx_is_normalized(gateway_client, monkeypatch):
    async def fake_send(**_kwargs):
        return httpx.Response(502, content=_provider_error_body()), 3, "provider_502"

    async def fake_dump(**_kwargs):
        return None

    monkeypatch.setattr(gateway, "_send_upstream_request_with_retries", fake_send)
    monkeypatch.setattr(gateway, "_dump_failed_call", fake_dump)

    async with gateway_client as client:
        response = await client.post(
            "/v1/chat/completions",
            headers={"X-Run-Id": "1"},
            json={"model": "openai/gpt-6-luna"},
        )

    assert response.status_code == 502
    assert response.json()["error"]["code"] == "provider_unavailable"
