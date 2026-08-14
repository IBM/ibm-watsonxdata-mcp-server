"""
Tests for DNS rebinding protection middleware.

This file has been modified with the assistance of IBM Bob AI tool
"""

import pytest
import httpx
from starlette.types import ASGIApp, Receive, Scope, Send

from lakehouse_mcp.http_security import DNSRebindingProtectionMiddleware


async def _ok_app(scope: Scope, receive: Receive, send: Send) -> None:
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-length", b"0")]})
    await send({"type": "http.response.body", "body": b"", "more_body": False})


def _client(app: ASGIApp) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=DNSRebindingProtectionMiddleware(app)),
        base_url="http://testserver",
    )


class TestDNSRebindingProtectionMiddleware:

    # --- blocked cases ---

    @pytest.mark.asyncio
    async def test_non_loopback_host_is_rejected(self):
        async with _client(_ok_app) as c:
            resp = await c.get("/mcp", headers={"Host": "attacker.com"})
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_non_loopback_host_with_port_is_rejected(self):
        async with _client(_ok_app) as c:
            resp = await c.get("/mcp", headers={"Host": "attacker.com:8080"})
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_non_loopback_origin_is_rejected(self):
        async with _client(_ok_app) as c:
            resp = await c.get("/mcp", headers={"Host": "localhost", "Origin": "http://attacker.com"})
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_non_loopback_origin_with_port_is_rejected(self):
        async with _client(_ok_app) as c:
            resp = await c.get("/mcp", headers={"Host": "localhost", "Origin": "http://attacker.com:1234"})
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_dns_rebind_pattern_blocked(self):
        # Attacker hostname resolves to 127.0.0.1 but browser still sends attacker origin
        async with _client(_ok_app) as c:
            resp = await c.get("/mcp", headers={"Host": "attacker.localtest.me", "Origin": "http://attacker.localtest.me"})
        assert resp.status_code == 403

    # --- allowed cases ---

    @pytest.mark.asyncio
    async def test_loopback_ip_host_is_allowed(self):
        async with _client(_ok_app) as c:
            resp = await c.get("/mcp", headers={"Host": "127.0.0.1"})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_loopback_ip_host_with_port_is_allowed(self):
        async with _client(_ok_app) as c:
            resp = await c.get("/mcp", headers={"Host": "127.0.0.1:8080"})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_localhost_host_is_allowed(self):
        async with _client(_ok_app) as c:
            resp = await c.get("/mcp", headers={"Host": "localhost"})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_localhost_host_with_port_is_allowed(self):
        async with _client(_ok_app) as c:
            resp = await c.get("/mcp", headers={"Host": "localhost:9000"})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_ipv6_loopback_host_is_allowed(self):
        async with _client(_ok_app) as c:
            resp = await c.get("/mcp", headers={"Host": "[::1]"})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_ipv6_loopback_host_with_port_is_allowed(self):
        async with _client(_ok_app) as c:
            resp = await c.get("/mcp", headers={"Host": "[::1]:8080"})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_loopback_origin_is_allowed(self):
        async with _client(_ok_app) as c:
            resp = await c.get("/mcp", headers={"Host": "localhost", "Origin": "http://localhost:8080"})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_loopback_ip_origin_is_allowed(self):
        async with _client(_ok_app) as c:
            resp = await c.get("/mcp", headers={"Host": "127.0.0.1", "Origin": "http://127.0.0.1:8080"})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_no_origin_header_is_allowed(self):
        # Non-browser MCP clients (curl, Claude Desktop) do not send Origin
        async with _client(_ok_app) as c:
            resp = await c.get("/mcp", headers={"Host": "localhost"})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_non_http_scope_is_passed_through(self):
        # Middleware must not interfere with websocket or lifespan scopes
        received: list[dict] = []

        async def capture_app(scope: Scope, receive: Receive, send: Send) -> None:
            received.append(scope)

        middleware = DNSRebindingProtectionMiddleware(capture_app)
        await middleware({"type": "lifespan"}, None, None)
        assert received == [{"type": "lifespan"}]
