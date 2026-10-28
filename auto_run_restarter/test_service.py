from auto_run_restarter.service import _classify_restart_reason


def test_provider_unavailable_is_restartable():
    assert _classify_restart_reason(
        "failed",
        "provider_unavailable (HTTP 502): Sorry, something went wrong.",
        tokens_used=None,
        input_tokens=None,
        cached_input_tokens=None,
        output_tokens=None,
        agent_steps=1,
    ) == "provider_502"
