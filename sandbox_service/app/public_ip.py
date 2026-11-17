"""The public IPv4 address this host reaches the internet from.

The sandbox names itself to the platform by it (SANDBOX_INSTANCE_ID, unless the
deployment sets one): it stays the same across redeploys, unlike the API container's
hostname, and it is the address operators know the host by.
"""
from __future__ import annotations

import ipaddress
import logging
import os

import httpx

logger = logging.getLogger(__name__)

# Each answers a GET with the bare IPv4 address of the caller. Asked in turn, so one
# being down does not leave the host unnamed.
DEFAULT_PUBLIC_IP_URLS = (
    "https://checkip.amazonaws.com",
    "https://api.ipify.org",
    "https://ipv4.icanhazip.com",
)
LOOKUP_TIMEOUT_SECONDS = 5.0


def public_ip_urls() -> tuple[str, ...]:
    """Lookup services, overridable as a comma-separated SANDBOX_PUBLIC_IP_URLS."""
    raw = os.getenv("SANDBOX_PUBLIC_IP_URLS") or ""
    urls = tuple(url.strip() for url in raw.split(",") if url.strip())
    return urls or DEFAULT_PUBLIC_IP_URLS


def parse_public_ip(text: str) -> str | None:
    """The address in a lookup service's answer; None unless it is a public IPv4.

    Anything else (an error page, a private address from a proxy) must not end up as
    the host's identity.
    """
    try:
        address = ipaddress.ip_address(text.strip())
    except ValueError:
        return None
    if address.version != 4 or not address.is_global:
        return None
    return str(address)


async def detect_public_ip(
    *,
    client: httpx.AsyncClient | None = None,
    urls: tuple[str, ...] | None = None,
) -> str | None:
    """Ask the lookup services in turn; the first public IPv4 one returns, else None."""
    owns_client = client is None
    client = client or httpx.AsyncClient(timeout=LOOKUP_TIMEOUT_SECONDS)
    try:
        for url in urls or public_ip_urls():
            try:
                response = await client.get(url, timeout=LOOKUP_TIMEOUT_SECONDS)
                response.raise_for_status()
            # Naming the host is observability: no lookup error may stop the API.
            except Exception as exc:
                logger.warning("Public IP lookup failed: url=%s error=%r", url, exc)
                continue
            address = parse_public_ip(response.text)
            if address is not None:
                return address
            logger.warning(
                "Public IP lookup returned no public IPv4: url=%s body=%r",
                url,
                response.text[:100],
            )
        return None
    finally:
        if owns_client:
            await client.aclose()
