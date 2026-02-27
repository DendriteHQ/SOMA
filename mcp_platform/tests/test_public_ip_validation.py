import os
import sys
import pytest

TESTS_DIR = os.path.dirname(__file__)
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if MCP_PLATFORM_DIR not in sys.path:
    sys.path.insert(0, MCP_PLATFORM_DIR)

from soma_shared.utils.verifier import is_public_ip


def test_public_ip_valid():
    """Test that public IPs are correctly identified."""
    assert is_public_ip("8.8.8.8") is True
    assert is_public_ip("1.1.1.1") is True
    assert is_public_ip("20.0.0.1") is True


def test_private_ip_invalid():
    """Test that private IPs are correctly identified as non-public."""
    assert is_public_ip("192.168.1.1") is False
    assert is_public_ip("10.0.0.1") is False
    assert is_public_ip("127.0.0.1") is False
    assert is_public_ip("172.16.0.1") is False


def test_loopback_invalid():
    """Test that loopback addresses are not public."""
    assert is_public_ip("127.0.0.1") is False
    assert is_public_ip("::1") is False


def test_invalid_ip():
    """Test that invalid IPs return False."""
    assert is_public_ip("not.an.ip") is False
    assert is_public_ip("999.999.999.999") is False
