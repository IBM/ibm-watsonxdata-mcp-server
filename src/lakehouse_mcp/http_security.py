"""
DNS rebinding protection middleware for Streamable HTTP transport.
"""

from __future__ import annotations

from urllib.parse import urlparse

from starlette.types import ASGIApp, Receive, Scope, Send

from lakehouse_mcp.observability import get_logger

logger = get_logger(__name__)

_LOOPBACK_HOSTNAMES: frozenset[str] = frozenset({"localhost", "127.0.0.1", "::1"})


def _extract_hostname(host: str) -> str:
    """Extract hostname from a Host header value, stripping any port number."""
    host = host.strip()
    if host.startswith("["):
        # IPv6 literal: [::1] or [::1]:port
        end = host.find("]")
        if end != -1:
            return host[1:end].lower()
        return host.lower()
    return host.split(":")[0].lower()


class DNSRebindingProtectionMiddleware:
    """Reject requests whose Host or Origin header is not a loopback address.

    Browsers always send the Origin header on cross-origin requests. After a
    DNS rebinding attack the attacker's hostname resolves to 127.0.0.1, but the
    browser still sends Origin: http://attacker.com. Rejecting non-loopback
    Origins closes the attack path without affecting legitimate non-browser
    clients (curl, MCP clients), which do not send Origin at all.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            headers: dict[bytes, bytes] = dict(scope.get("headers", []))

            host_raw = headers.get(b"host", b"").decode("latin-1")
            if host_raw:
                hostname = _extract_hostname(host_raw)
                if hostname not in _LOOPBACK_HOSTNAMES:
                    logger.warning(
                        "dns_rebinding_blocked",
                        reason="non_loopback_host",
                        host=host_raw,
                    )
                    await self._reject(send, "Forbidden: non-loopback Host header rejected")
                    return

            origin_raw = headers.get(b"origin", b"").decode("latin-1")
            if origin_raw:
                parsed = urlparse(origin_raw)
                origin_hostname = _extract_hostname(parsed.hostname or "")
                if origin_hostname not in _LOOPBACK_HOSTNAMES:
                    logger.warning(
                        "dns_rebinding_blocked",
                        reason="non_loopback_origin",
                        origin=origin_raw,
                    )
                    await self._reject(send, "Forbidden: non-loopback Origin header rejected")
                    return

        await self.app(scope, receive, send)

    @staticmethod
    async def _reject(send: Send, message: str) -> None:
        body = message.encode("utf-8")
        await send({
            "type": "http.response.start",
            "status": 403,
            "headers": [
                (b"content-type", b"text/plain; charset=utf-8"),
                (b"content-length", str(len(body)).encode()),
            ],
        })
        await send({
            "type": "http.response.body",
            "body": body,
            "more_body": False,
        })
