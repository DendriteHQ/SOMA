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
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gateway")


@pytest.mark.parametrize("stream", [False, True])
async def test_gpt6_luna_reasoning_effort_is_pinned(gateway_client, monkeypatch, stream):
    sent = {}

    async def fake_send(**kwargs):
        sent.update(json.loads(kwargs["body_bytes"]))
        return httpx.Response(200, json={"ok": True}), 1, None

    async def fake_stream(**kwargs):
        sent.update(json.loads(kwargs["body_bytes"]))

        async def chunks():
            yield b"data: done\n\n"

        return chunks(), 200, {"content-type": "text/event-stream"}, httpx.Headers(), None, 1, None

    monkeypatch.setattr(gateway, "_send_upstream_request_with_retries", fake_send)
    monkeypatch.setattr(gateway, "_stream_upstream_response", fake_stream)

    async with gateway_client as client:
        response = await client.post(
            "/v1/chat/completions",
            headers={"X-Run-Id": "1"},
            json={
                "model": "openai/gpt-6-luna",
                "stream": stream,
                "reasoning_effort": "high",
                "reasoning": {"effort": "low", "max_tokens": 1000, "enabled": False, "exclude": True},
            },
        )

    assert response.status_code == 200
    assert sent["reasoning"] == {"exclude": True, "effort": "medium"}
    assert "reasoning_effort" not in sent
    assert sent["provider"] == {"order": ["openai"], "allow_fallbacks": False}


@pytest.mark.parametrize(
    ("path", "model"),
    [("chat/completions", "other/model"), ("systemone", "openai/gpt-6-luna")],
)
async def test_other_requests_keep_reasoning(gateway_client, monkeypatch, path, model):
    sent = {}

    async def fake_send(**kwargs):
        sent.update(json.loads(kwargs["body_bytes"]))
        return httpx.Response(200, json={"ok": True}), 1, None

    monkeypatch.setattr(gateway, "_send_upstream_request_with_retries", fake_send)

    async with gateway_client as client:
        response = await client.post(
            f"/v1/{path}",
            headers={"X-Run-Id": "1"},
            json={"model": model, "reasoning_effort": "high", "reasoning": {"effort": "low"}},
        )

    assert response.status_code == 200
    assert sent["reasoning_effort"] == "high"
    assert sent["reasoning"] == {"effort": "low"}
    assert ("provider" in sent) is (path != "systemone")
