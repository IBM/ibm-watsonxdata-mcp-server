"""
watsonx.data REST API client with IBM IAM authentication.

This module provides async HTTP client for watsonx.data API with:
- IBM Cloud IAM authentication
- Automatic token refresh
- OpenTelemetry instrumentation
- Structured logging

This file has been modified with the assistance of IBM Bob AI tool
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import httpx
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator

from lakehouse_mcp.observability import get_logger, get_tracer

if TYPE_CHECKING:
    from lakehouse_mcp.config import WatsonXConfig

logger = get_logger(__name__)
tracer = get_tracer(__name__)


class WatsonXClient:
    """Async HTTP client for watsonx.data API."""

    def __init__(self, config: WatsonXConfig) -> None:
        """Initialize WatsonX client.

        Args:
            config: WatsonX configuration
        """
        self.config = config
        self.logger = logger

        # Create IBM IAM authenticator
        self.authenticator = IAMAuthenticator(
            apikey=config.api_key,
            disable_ssl_verification=config.tls_insecure_skip_verify,
        )

        # Create async HTTP client with httpx
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(config.timeout_seconds),
            verify=not config.tls_insecure_skip_verify,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "AuthInstanceId": config.instance_id,
            },
        )

        logger.debug(
            "watsonx_client_initialized",
            base_url=config.base_url,
            timeout=config.timeout_seconds,
        )

    async def __aenter__(self) -> WatsonXClient:
        """Async context manager entry."""
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Async context manager exit."""
        await self.close()

    async def close(self) -> None:
        """Close the HTTP client."""
        await self.client.aclose()

    async def _get_auth_header(self) -> dict[str, str]:
        """Get IBM IAM authorization header.

        Returns:
            Authorization header dict
        """
        # IBM SDK's token_manager handles automatic refresh
        token = self.authenticator.token_manager.get_token()
        return {"Authorization": f"Bearer {token}"}

    async def get(self, path: str) -> dict[str, Any]:
        """Perform GET request to watsonx.data API.

        Args:
            path: API path (relative or absolute URL)

        Returns:
            Response JSON as dictionary

        Raises:
            httpx.HTTPStatusError: For HTTP error responses
            httpx.RequestError: For network errors
        """
        with tracer.start_as_current_span("watsonx.get") as span:
            span.set_attribute("http.path", path)

            # Build full URL
            url = path if path.startswith("http") else f"{self.config.base_url}{path}"

            # Get authorization header
            auth_headers = await self._get_auth_header()

            logger.info(
                "watsonx_get_request",
                url=url,
                path=path,
            )

            # Make request
            response = await self.client.get(url, headers=auth_headers)

            span.set_attribute("http.status_code", response.status_code)

            # Check status
            if response.status_code >= 400:
                try:
                    error_data = response.json()
                    error_msg = error_data.get("message", error_data.get("exception", str(error_data)))
                    logger.error("watsonx_get_error", url=url, status_code=response.status_code, error=error_msg, response=error_data)
                    raise RuntimeError(f"API Error: {error_msg}\nFull response: {error_data}")
                except (ValueError, KeyError):
                    response.raise_for_status()

            data = response.json()

            logger.info(
                "watsonx_get_success",
                url=url,
                status_code=response.status_code,
            )

            return data

    async def post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        """Perform POST request to watsonx.data API.

        Args:
            path: API path (relative or absolute URL)
            body: Request body as dictionary

        Returns:
            Response JSON as dictionary

        Raises:
            httpx.HTTPStatusError: For HTTP error responses
            httpx.RequestError: For network errors
        """
        with tracer.start_as_current_span("watsonx.post") as span:
            span.set_attribute("http.path", path)

            # Build full URL
            url = path if path.startswith("http") else f"{self.config.base_url}{path}"

            # Get authorization header
            auth_headers = await self._get_auth_header()

            logger.info(
                "watsonx_post_request",
                url=url,
                path=path,
            )

            # Make request
            response = await self.client.post(url, json=body, headers=auth_headers)

            span.set_attribute("http.status_code", response.status_code)

            # Check status (200, 201, 202 are all success)
            if response.status_code >= 400:
                # Try to get error details from response body
                try:
                    error_data = response.json()
                    error_msg = error_data.get("message", error_data.get("exception", str(error_data)))
                    logger.error("watsonx_post_error", url=url, status_code=response.status_code, error=error_msg, response=error_data)
                    # Raise with full error details in the message
                    raise RuntimeError(f"API Error: {error_msg}\nFull response: {error_data}")
                except (ValueError, KeyError):
                    # If we can't parse JSON, use default error handling
                    response.raise_for_status()
            
            data = response.json()

            logger.info(
                "watsonx_post_success",
                url=url,
                status_code=response.status_code,
            )

            return data

    async def patch(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        """Perform PATCH request to watsonx.data API.

        Used for updating existing resources (engines, configurations, etc.).

        Args:
            path: API path (relative or absolute URL)
            body: Request body as dictionary with fields to update

        Returns:
            Response JSON as dictionary

        Raises:
            httpx.HTTPStatusError: For HTTP error responses
            httpx.RequestError: For network errors

        Example:
            >>> client = WatsonXClient(config)
            >>> await client.patch(
            ...     "/v2/presto_engines/engine-123",
            ...     {"description": "Updated description"}
            ... )
        """
        with tracer.start_as_current_span("watsonx.patch") as span:
            span.set_attribute("http.path", path)

            # Build full URL
            url = path if path.startswith("http") else f"{self.config.base_url}{path}"

            # Get authorization header
            auth_headers = await self._get_auth_header()

            logger.info(
                "watsonx_patch_request",
                url=url,
                path=path,
            )

            # Make request
            response = await self.client.patch(url, json=body, headers=auth_headers)

            span.set_attribute("http.status_code", response.status_code)

            # Check status
            if response.status_code >= 400:
                try:
                    error_data = response.json()
                    error_msg = error_data.get("message", error_data.get("exception", str(error_data)))
                    logger.error("watsonx_patch_error", url=url, status_code=response.status_code, error=error_msg, response=error_data)
                    raise RuntimeError(f"API Error: {error_msg}\nFull response: {error_data}")
                except (ValueError, KeyError):
                    response.raise_for_status()

            data = response.json()

            logger.info(
                "watsonx_patch_success",
                url=url,
                status_code=response.status_code,
            )

            return data

    async def delete(self, path: str) -> dict[str, Any]:
        """Perform DELETE request to watsonx.data API.

        Used for deleting resources (engines, applications, jobs, etc.).

        Args:
            path: API path (relative or absolute URL)

        Returns:
            Response JSON as dictionary (may be empty for 204 responses)

        Raises:
            httpx.HTTPStatusError: For HTTP error responses
            httpx.RequestError: For network errors

        Example:
            >>> client = WatsonXClient(config)
            >>> await client.delete("/v2/presto_engines/engine-123")
        """
        with tracer.start_as_current_span("watsonx.delete") as span:
            span.set_attribute("http.path", path)

            # Build full URL
            url = path if path.startswith("http") else f"{self.config.base_url}{path}"

            # Get authorization header
            auth_headers = await self._get_auth_header()

            logger.info(
                "watsonx_delete_request",
                url=url,
                path=path,
            )

            # Make request
            response = await self.client.delete(url, headers=auth_headers)

            span.set_attribute("http.status_code", response.status_code)

            # Check status
            if response.status_code >= 400:
                try:
                    error_data = response.json()
                    error_msg = error_data.get("message", error_data.get("exception", str(error_data)))
                    logger.error("watsonx_delete_error", url=url, status_code=response.status_code, error=error_msg, response=error_data)
                    raise RuntimeError(f"API Error: {error_msg}\nFull response: {error_data}")
                except (ValueError, KeyError):
                    response.raise_for_status()

            # Handle 204 No Content responses
            if response.status_code == 204:
                data = {}
            else:
                data = response.json()

            logger.info(
                "watsonx_delete_success",
                url=url,
                status_code=response.status_code,
            )

            return data
