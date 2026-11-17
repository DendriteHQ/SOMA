import pytest

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


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        ("OSError: [Errno 24] Too many open files", "too_many_open_files"),
        ("'NoneType' object has no attribute 'read'", "nonetype_read_attribute_error"),
        ("[Errno -3] Temporary failure in name resolution", "dns_temporary_failure"),
        ("httpx.ConnectError: All connection attempts failed", "all_connection_attempts_failed"),
        ("[Errno 104] Connection reset by peer", "connection_reset_by_peer"),
        ("'utf-8' codec can't decode byte 0xff in position 0", "utf8_decode_error"),
        ("Pool overlaps with other one on this address space: networks have overlapping IPv4", "docker_network_overlap"),
        ("COMMAND FAILED WITH EXIT CODE 1: git apply", "command_failed_exit_code_1"),
    ],
)
def test_case_insensitive_patterns_are_restartable(error, reason):
    assert _classify_restart_reason(
        "failed",
        error,
        tokens_used=10,
        input_tokens=None,
        cached_input_tokens=None,
        output_tokens=None,
        agent_steps=3,
    ) == reason


def test_nonetype_pattern_requires_read():
    assert _classify_restart_reason(
        "failed",
        "'NoneType' object has no attribute 'split'",
        tokens_used=None,
        input_tokens=None,
        cached_input_tokens=None,
        output_tokens=None,
        agent_steps=None,
    ) is None
