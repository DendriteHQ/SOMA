from __future__ import annotations

import os
from dataclasses import dataclass

import httpx
import pytest


PUBLIC_PREFIX = "/api/public/frontend-key"


@dataclass(slots=True)
class FrontendApiContext:
    competition_id: int | None
    hotkey: str | None
    batch_challenge_id: int | None


def _response_debug(response: httpx.Response) -> str:
    text = response.text
    if len(text) > 500:
        text = f"{text[:500]}...<truncated>"
    return f"status={response.status_code} body={text}"


@pytest.fixture(scope="session")
def frontend_api_base_url() -> str:
    return os.getenv("FRONTEND_API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")


@pytest.fixture(scope="session")
def frontend_api_key() -> str:
    key = os.getenv("FRONTEND_API_KEY")
    if not key:
        pytest.skip(
            "Set FRONTEND_API_KEY before running these integration tests.",
        )
    return key


@pytest.fixture(scope="session")
def frontend_client(
    frontend_api_base_url: str,
    frontend_api_key: str,
) -> httpx.Client:
    client = httpx.Client(
        base_url=frontend_api_base_url,
        headers={"x-api-key": frontend_api_key},
        timeout=30.0,
    )
    try:
        # Quick connectivity/auth sanity check.
        response = client.get(f"{PUBLIC_PREFIX}/summary")
        assert response.status_code == 200, (
            "Failed basic authenticated request to frontend API: "
            f"{_response_debug(response)}"
        )
        yield client
    finally:
        client.close()


def _resolve_context(client: httpx.Client) -> FrontendApiContext:
    competition_id: int | None = None
    hotkey: str | None = None
    batch_challenge_id: int | None = None

    timeframe_response = client.get(f"{PUBLIC_PREFIX}/competition/timeframe/current")
    if timeframe_response.status_code == 200:
        timeframe_payload = timeframe_response.json()
        raw_competition_id = timeframe_payload.get("competition_id")
        if isinstance(raw_competition_id, int):
            competition_id = raw_competition_id

    competitions_response = client.get(f"{PUBLIC_PREFIX}/competitions-list")
    assert competitions_response.status_code == 200, _response_debug(competitions_response)
    competitions_payload = competitions_response.json()
    if competition_id is None and isinstance(competitions_payload, list) and competitions_payload:
        first_competition = competitions_payload[0]
        if isinstance(first_competition, dict):
            raw_competition_id = first_competition.get("competition_id")
            if isinstance(raw_competition_id, int):
                competition_id = raw_competition_id

    if competition_id is None:
        return FrontendApiContext(None, None, None)

    miners_response = client.get(
        f"{PUBLIC_PREFIX}/miners/{competition_id}",
        params={"page": 1, "limit": 20},
    )
    assert miners_response.status_code == 200, _response_debug(miners_response)
    miners_payload = miners_response.json()
    miners = miners_payload.get("miners", []) if isinstance(miners_payload, dict) else []
    if miners and isinstance(miners[0], dict):
        raw_hotkey = miners[0].get("hotkey")
        if isinstance(raw_hotkey, str) and raw_hotkey:
            hotkey = raw_hotkey

    if hotkey is None:
        return FrontendApiContext(competition_id, None, None)

    miner_comp_challenges_response = client.get(
        f"{PUBLIC_PREFIX}/miners/{competition_id}/{hotkey}/competition/challenges",
    )
    assert miner_comp_challenges_response.status_code == 200, _response_debug(
        miner_comp_challenges_response
    )
    miner_comp_challenges_payload = miner_comp_challenges_response.json()
    challenges = (
        miner_comp_challenges_payload.get("challenges", [])
        if isinstance(miner_comp_challenges_payload, dict)
        else []
    )
    if challenges and isinstance(challenges[0], dict):
        raw_batch_challenge_id = challenges[0].get("batch_challenge_id")
        if isinstance(raw_batch_challenge_id, int):
            batch_challenge_id = raw_batch_challenge_id

    return FrontendApiContext(competition_id, hotkey, batch_challenge_id)


@pytest.fixture(scope="session")
def frontend_api_context(frontend_client: httpx.Client) -> FrontendApiContext:
    return _resolve_context(frontend_client)


@pytest.mark.network
def test_frontend_key_rejects_missing_api_key(frontend_api_base_url: str) -> None:
    response = httpx.get(
        f"{frontend_api_base_url}{PUBLIC_PREFIX}/summary",
        timeout=30.0,
    )
    assert response.status_code == 401, _response_debug(response)


@pytest.mark.network
def test_frontend_key_rejects_invalid_api_key(frontend_api_base_url: str) -> None:
    response = httpx.get(
        f"{frontend_api_base_url}{PUBLIC_PREFIX}/summary",
        headers={"x-api-key": "soma_invalidprefix.invalidsecret"},
        timeout=30.0,
    )
    assert response.status_code == 401, _response_debug(response)


@pytest.mark.network
def test_frontend_key_summary(frontend_client: httpx.Client) -> None:
    response = frontend_client.get(f"{PUBLIC_PREFIX}/summary")
    assert response.status_code == 200, _response_debug(response)
    payload = response.json()
    assert isinstance(payload, dict)
    for key in [
        "server_ts",
        "miners",
        "validators",
        "active_validators",
        "competitions",
        "active_competitions",
    ]:
        assert key in payload


@pytest.mark.network
def test_frontend_key_timeframe(frontend_client: httpx.Client) -> None:
    response = frontend_client.get(f"{PUBLIC_PREFIX}/competition/timeframe/current")
    # A 404 can be valid when no active competition exists.
    assert response.status_code in {200, 404}, _response_debug(response)
    if response.status_code == 200:
        payload = response.json()
        assert isinstance(payload.get("competition_id"), int)


@pytest.mark.network
def test_frontend_key_competitions_list(frontend_client: httpx.Client) -> None:
    response = frontend_client.get(f"{PUBLIC_PREFIX}/competitions-list")
    assert response.status_code == 200, _response_debug(response)
    payload = response.json()
    assert isinstance(payload, list)


@pytest.mark.network
def test_frontend_key_validators(frontend_client: httpx.Client) -> None:
    response = frontend_client.get(f"{PUBLIC_PREFIX}/validators")
    assert response.status_code == 200, _response_debug(response)
    payload = response.json()
    assert isinstance(payload, dict)
    assert "validators" in payload
    assert isinstance(payload["validators"], list)


@pytest.mark.network
def test_frontend_key_miners_list(
    frontend_client: httpx.Client,
    frontend_api_context: FrontendApiContext,
) -> None:
    if frontend_api_context.competition_id is None:
        pytest.skip("No active competition discovered for miner list endpoint test.")

    response = frontend_client.get(
        f"{PUBLIC_PREFIX}/miners/{frontend_api_context.competition_id}",
        params={"page": 1, "limit": 20},
    )
    assert response.status_code == 200, _response_debug(response)
    payload = response.json()
    assert isinstance(payload, dict)
    assert "miners" in payload
    assert "pagination" in payload


@pytest.mark.network
def test_frontend_key_miner_detail(
    frontend_client: httpx.Client,
    frontend_api_context: FrontendApiContext,
) -> None:
    if frontend_api_context.competition_id is None or frontend_api_context.hotkey is None:
        pytest.skip("No miner discovered for miner detail endpoint test.")

    response = frontend_client.get(
        f"{PUBLIC_PREFIX}/miners/{frontend_api_context.competition_id}/{frontend_api_context.hotkey}",
    )
    assert response.status_code == 200, _response_debug(response)
    payload = response.json()
    assert isinstance(payload, dict)
    assert "miner" in payload
    assert "last_contest" in payload


@pytest.mark.network
def test_frontend_key_miner_competition(
    frontend_client: httpx.Client,
    frontend_api_context: FrontendApiContext,
) -> None:
    if frontend_api_context.competition_id is None or frontend_api_context.hotkey is None:
        pytest.skip("No miner discovered for miner competition endpoint test.")

    response = frontend_client.get(
        f"{PUBLIC_PREFIX}/miners/{frontend_api_context.competition_id}/{frontend_api_context.hotkey}/competition",
    )
    # 404 is valid when this miner has no competition aggregate row yet.
    assert response.status_code in {200, 404}, _response_debug(response)
    if response.status_code == 200:
        payload = response.json()
        assert isinstance(payload, dict)
        for key in ["id", "name"]:
            assert key in payload


@pytest.mark.network
def test_frontend_key_miner_competition_challenges(
    frontend_client: httpx.Client,
    frontend_api_context: FrontendApiContext,
) -> None:
    if frontend_api_context.competition_id is None or frontend_api_context.hotkey is None:
        pytest.skip("No miner discovered for miner competition challenges endpoint test.")

    response = frontend_client.get(
        f"{PUBLIC_PREFIX}/miners/{frontend_api_context.competition_id}/{frontend_api_context.hotkey}/competition/challenges",
    )
    assert response.status_code == 200, _response_debug(response)
    payload = response.json()
    assert isinstance(payload, dict)
    assert "challenges" in payload
    assert "total" in payload


@pytest.mark.network
def test_frontend_key_miner_challenge_detail_when_available(
    frontend_client: httpx.Client,
    frontend_api_context: FrontendApiContext,
) -> None:
    if (
        frontend_api_context.competition_id is None
        or frontend_api_context.hotkey is None
        or frontend_api_context.batch_challenge_id is None
    ):
        pytest.skip("No batch challenge discovered for challenge detail endpoint test.")

    response = frontend_client.get(
        f"{PUBLIC_PREFIX}/miners/{frontend_api_context.hotkey}/competition/challenges/{frontend_api_context.batch_challenge_id}",
    )
    assert response.status_code == 200, _response_debug(response)
    payload = response.json()
    assert isinstance(payload, dict)
    assert "challenge" in payload


@pytest.mark.network
def test_frontend_key_miner_screener(
    frontend_client: httpx.Client,
    frontend_api_context: FrontendApiContext,
) -> None:
    if frontend_api_context.competition_id is None or frontend_api_context.hotkey is None:
        pytest.skip("No miner discovered for miner screener endpoint test.")

    response = frontend_client.get(
        f"{PUBLIC_PREFIX}/miners/{frontend_api_context.competition_id}/{frontend_api_context.hotkey}/screener",
    )
    # 404 is valid when miner has no screener participation.
    assert response.status_code in {200, 404}, _response_debug(response)
    if response.status_code == 200:
        payload = response.json()
        assert isinstance(payload, dict)
        assert "id" in payload
        assert "name" in payload


@pytest.mark.network
def test_frontend_key_miner_screener_challenges(
    frontend_client: httpx.Client,
    frontend_api_context: FrontendApiContext,
) -> None:
    if frontend_api_context.competition_id is None or frontend_api_context.hotkey is None:
        pytest.skip("No miner discovered for miner screener challenges endpoint test.")

    response = frontend_client.get(
        f"{PUBLIC_PREFIX}/miners/{frontend_api_context.competition_id}/{frontend_api_context.hotkey}/screener/challenges",
    )
    # 404 is valid when miner has no screener participation.
    assert response.status_code in {200, 404}, _response_debug(response)
    if response.status_code == 200:
        payload = response.json()
        assert isinstance(payload, dict)
        assert "challenges" in payload
        assert "total" in payload
