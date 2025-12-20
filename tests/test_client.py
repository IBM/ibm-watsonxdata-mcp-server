"""
Tests for watsonx.data client.

This file has been modified with the assistance of IBM Bob AI tool
"""

import httpx
import pytest

from lakehouse_mcp.client.watsonx import WatsonXClient


class TestWatsonXClient:
    """Tests for WatsonXClient."""

    @pytest.mark.asyncio
    async def test_client_initialization(self, watsonx_config, mock_iam_authenticator):
        """Test client initialization."""
        client = WatsonXClient(watsonx_config)
        client.authenticator = mock_iam_authenticator

        assert client.config == watsonx_config
        assert client.authenticator == mock_iam_authenticator
        assert isinstance(client.client, httpx.AsyncClient)

        await client.close()

    @pytest.mark.asyncio
    async def test_context_manager(self, watsonx_config, mock_iam_authenticator):
        """Test async context manager."""
        async with WatsonXClient(watsonx_config) as client:
            client.authenticator = mock_iam_authenticator
            assert isinstance(client, WatsonXClient)

        # Client should be closed after context
        assert client.client.is_closed

    @pytest.mark.asyncio
    async def test_get_auth_header(self, watsonx_client):
        """Test getting authorization header."""
        header = await watsonx_client._get_auth_header()

        assert "Authorization" in header
        assert header["Authorization"] == "Bearer mock_access_token_123"

    @pytest.mark.asyncio
    async def test_get_request_relative_path(self, watsonx_client, respx_mock):
        """Test GET request with relative path."""
        mock_response = {"status": "ok", "data": [1, 2, 3]}

        respx_mock.get("https://test.watsonx.com/api/v2/engines").mock(return_value=httpx.Response(200, json=mock_response))

        result = await watsonx_client.get("/v2/engines")

        assert result == mock_response

    @pytest.mark.asyncio
    async def test_get_request_absolute_url(self, watsonx_client, respx_mock):
        """Test GET request with absolute URL."""
        mock_response = {"status": "ok"}
        absolute_url = "https://other.service.com/api/v1/data"

        respx_mock.get(absolute_url).mock(return_value=httpx.Response(200, json=mock_response))

        result = await watsonx_client.get(absolute_url)

        assert result == mock_response

    @pytest.mark.asyncio
    async def test_get_request_with_auth_headers(self, watsonx_client, respx_mock):
        """Test that GET request includes authorization headers."""
        mock_response = {"status": "ok"}

        route = respx_mock.get("https://test.watsonx.com/api/v2/test").mock(return_value=httpx.Response(200, json=mock_response))

        await watsonx_client.get("/v2/test")

        # Verify the authorization header was sent
        assert route.called
        request = route.calls[0].request
        assert "Authorization" in request.headers
        assert request.headers["Authorization"] == "Bearer mock_access_token_123"

    @pytest.mark.asyncio
    async def test_get_request_404_error(self, watsonx_client, respx_mock):
        """Test GET request with 404 error."""
        respx_mock.get("https://test.watsonx.com/api/v2/notfound").mock(return_value=httpx.Response(404, json={"error": "Not found"}))

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await watsonx_client.get("/v2/notfound")

        assert exc_info.value.response.status_code == 404

    @pytest.mark.asyncio
    async def test_get_request_500_error(self, watsonx_client, respx_mock):
        """Test GET request with 500 error."""
        respx_mock.get("https://test.watsonx.com/api/v2/error").mock(
            return_value=httpx.Response(500, json={"error": "Internal server error"})
        )

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await watsonx_client.get("/v2/error")

        assert exc_info.value.response.status_code == 500

    @pytest.mark.asyncio
    async def test_get_request_timeout(self, watsonx_client, respx_mock):
        """Test GET request timeout."""
        respx_mock.get("https://test.watsonx.com/api/v2/slow").mock(side_effect=httpx.TimeoutException("Request timed out"))

        with pytest.raises(httpx.TimeoutException):
            await watsonx_client.get("/v2/slow")

    @pytest.mark.asyncio
    async def test_post_request_relative_path(self, watsonx_client, respx_mock):
        """Test POST request with relative path."""
        request_body = {"query": "SELECT * FROM table"}
        mock_response = {"query_id": "123", "status": "running"}

        respx_mock.post("https://test.watsonx.com/api/v3/execute").mock(return_value=httpx.Response(200, json=mock_response))

        result = await watsonx_client.post("/v3/execute", request_body)

        assert result == mock_response

    @pytest.mark.asyncio
    async def test_post_request_absolute_url(self, watsonx_client, respx_mock):
        """Test POST request with absolute URL."""
        request_body = {"data": "test"}
        mock_response = {"id": "456"}
        absolute_url = "https://other.service.com/api/v1/submit"

        respx_mock.post(absolute_url).mock(return_value=httpx.Response(201, json=mock_response))

        result = await watsonx_client.post(absolute_url, request_body)

        assert result == mock_response

    @pytest.mark.asyncio
    async def test_post_request_with_body(self, watsonx_client, respx_mock):
        """Test that POST request sends body correctly."""
        request_body = {"sql": "SELECT 1", "catalog": "test"}
        mock_response = {"query_id": "789"}

        route = respx_mock.post("https://test.watsonx.com/api/v3/query").mock(return_value=httpx.Response(200, json=mock_response))

        await watsonx_client.post("/v3/query", request_body)

        # Verify the request body was sent
        assert route.called
        request = route.calls[0].request
        assert request.method == "POST"
        # httpx sends json body, verify it's there
        assert b"SELECT 1" in request.content

    @pytest.mark.asyncio
    async def test_post_request_201_response(self, watsonx_client, respx_mock):
        """Test POST request with 201 Created response."""
        request_body = {"name": "new_engine"}
        mock_response = {"id": "engine-123", "status": "creating"}

        respx_mock.post("https://test.watsonx.com/api/v2/engines").mock(return_value=httpx.Response(201, json=mock_response))

        result = await watsonx_client.post("/v2/engines", request_body)

        assert result == mock_response

    @pytest.mark.asyncio
    async def test_post_request_202_response(self, watsonx_client, respx_mock):
        """Test POST request with 202 Accepted response."""
        request_body = {"action": "start"}
        mock_response = {"status": "accepted"}

        respx_mock.post("https://test.watsonx.com/api/v2/engines/start").mock(return_value=httpx.Response(202, json=mock_response))

        result = await watsonx_client.post("/v2/engines/start", request_body)

        assert result == mock_response

    @pytest.mark.asyncio
    async def test_post_request_400_error(self, watsonx_client, respx_mock):
        """Test POST request with 400 Bad Request error."""
        request_body = {"invalid": "data"}

        respx_mock.post("https://test.watsonx.com/api/v3/query").mock(return_value=httpx.Response(400, json={"error": "Invalid request"}))

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await watsonx_client.post("/v3/query", request_body)

        assert exc_info.value.response.status_code == 400

    @pytest.mark.asyncio
    async def test_post_request_with_auth_headers(self, watsonx_client, respx_mock):
        """Test that POST request includes authorization headers."""
        request_body = {"test": "data"}
        mock_response = {"status": "ok"}

        route = respx_mock.post("https://test.watsonx.com/api/v2/test").mock(return_value=httpx.Response(200, json=mock_response))

        await watsonx_client.post("/v2/test", request_body)

        # Verify the authorization header was sent
        assert route.called
        request = route.calls[0].request
        assert "Authorization" in request.headers
        assert request.headers["Authorization"] == "Bearer mock_access_token_123"

    @pytest.mark.asyncio
    async def test_close_client(self, watsonx_client):
        """Test closing the client."""
        assert not watsonx_client.client.is_closed

        await watsonx_client.close()

        assert watsonx_client.client.is_closed

    @pytest.mark.asyncio
    async def test_instance_id_header(self, watsonx_client, respx_mock):
        """Test that AuthInstanceId header is set correctly."""
        mock_response = {"status": "ok"}

        route = respx_mock.get("https://test.watsonx.com/api/v2/test").mock(return_value=httpx.Response(200, json=mock_response))

        await watsonx_client.get("/v2/test")

        # Verify the AuthInstanceId header was sent
        assert route.called
        request = route.calls[0].request
        assert "AuthInstanceId" in request.headers
        assert request.headers["AuthInstanceId"] == "crn:v1:bluemix:public:lakehouse:us-south:a/test123:instance456::"

    @pytest.mark.asyncio
    async def test_content_type_headers(self, watsonx_client, respx_mock):
        """Test that Content-Type and Accept headers are set correctly."""
        mock_response = {"status": "ok"}

        route = respx_mock.get("https://test.watsonx.com/api/v2/test").mock(return_value=httpx.Response(200, json=mock_response))

        await watsonx_client.get("/v2/test")

        # Verify headers
        assert route.called
        request = route.calls[0].request
        assert "Content-Type" in request.headers
        assert request.headers["Content-Type"] == "application/json"
        assert "Accept" in request.headers
        assert request.headers["Accept"] == "application/json"
