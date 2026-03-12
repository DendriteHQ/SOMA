import os
import sys

TESTS_DIR = os.path.dirname(__file__)
MCP_PLATFORM_DIR = os.path.abspath(os.path.join(TESTS_DIR, ".."))
if MCP_PLATFORM_DIR not in sys.path:
    sys.path.insert(0, MCP_PLATFORM_DIR)

os.environ.setdefault("PRIVATE_NETWORK_CIDRS", '["127.0.0.1/32"]')
os.environ.setdefault("TRUSTED_PROXY_CIDRS", '["127.0.0.1/32"]')

from app.api.routes.utils import _is_compressed_enough


def test_is_compressed_enough_rejects_empty_compressed_text():
    assert not _is_compressed_enough(
        original="this is original text",
        compressed="",
        ratio=1.0,
    )
