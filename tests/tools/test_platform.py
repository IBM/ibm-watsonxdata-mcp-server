"""
Tests for platform tools.

This file has been modified with the assistance of IBM Bob AI tool
"""

import httpx
import pytest

from lakehouse_mcp.tools.platform.get_instance_details import get_instance_details


class TestGetInstanceDetails:
    """Tests for get_instance_details tool."""

    @pytest.mark.asyncio
    async def test_get_instance_details_success(self, mock_context, watsonx_client, respx_mock, mock_instance_response):
        """Test successful instance details retrieval."""
        respx_mock.get("https://test.watsonx.com/api/v2/instance").mock(return_value=httpx.Response(200, json=mock_instance_response))

        result = await get_instance_details.fn(mock_context)

        assert result["instance_id"] == "crn:v1:bluemix:public:lakehouse:us-south:a/test:inst123::"
        assert result["region"] == "us-south"
        assert result["cloud_type"] == "ibm-cloud"
        assert result["account_type"] == "ENTERPRISE"
        assert result["plan_id"] == "lakehouse-enterprise"
        assert result["status"] == "active"
        assert result["instance_status"] == "deployed"
        assert result["version"] == "2.0.0"
        assert result["public_endpoints_enabled"] is True
        assert result["private_endpoints_enabled"] is False
        assert result["serverless_spark_enabled"] is True
        assert result["resource_group_crn"] == "crn:v1:bluemix:public:resource-controller::a/test:::resource-group:rg123"
        assert result["console_url"] == "https://console.watsonx.test.com"
        assert result["first_time_use"] is False

    @pytest.mark.asyncio
    async def test_get_instance_details_missing_deployment(self, mock_context, watsonx_client, respx_mock):
        """Test instance details when deployment is missing."""
        # Response without deployment
        response = {"deploymentresponse": {}}

        respx_mock.get("https://test.watsonx.com/api/v2/instance").mock(return_value=httpx.Response(200, json=response))

        result = await get_instance_details.fn(mock_context)

        # All fields should have default values
        assert result["instance_id"] == "unknown"
        assert result["region"] == "unknown"
        assert result["status"] == "unknown"
        assert result["public_endpoints_enabled"] is False
        assert result["serverless_spark_enabled"] is False

    @pytest.mark.asyncio
    async def test_get_instance_details_partial_data(self, mock_context, watsonx_client, respx_mock):
        """Test instance details with partial data."""
        response = {
            "deploymentresponse": {
                "deployment": {
                    "id": "crn:test",
                    "region": "us-east",
                    "status": "active",
                    # Missing other fields
                }
            }
        }

        respx_mock.get("https://test.watsonx.com/api/v2/instance").mock(return_value=httpx.Response(200, json=response))

        result = await get_instance_details.fn(mock_context)

        assert result["instance_id"] == "crn:test"
        assert result["region"] == "us-east"
        assert result["status"] == "active"
        # Missing fields should have defaults
        assert result["cloud_type"] == "unknown"
        assert result["account_type"] == "unknown"
        assert result["version"] == "unknown"
        assert result["public_endpoints_enabled"] is False

    @pytest.mark.asyncio
    async def test_get_instance_details_api_error(self, mock_context, watsonx_client, respx_mock):
        """Test instance details when API returns error."""
        respx_mock.get("https://test.watsonx.com/api/v2/instance").mock(
            return_value=httpx.Response(500, json={"error": "Internal server error"})
        )

        with pytest.raises(httpx.HTTPStatusError):
            await get_instance_details.fn(mock_context)

    @pytest.mark.asyncio
    async def test_get_instance_details_not_found(self, mock_context, watsonx_client, respx_mock):
        """Test instance details when instance not found."""
        respx_mock.get("https://test.watsonx.com/api/v2/instance").mock(
            return_value=httpx.Response(404, json={"error": "Instance not found"})
        )

        with pytest.raises(httpx.HTTPStatusError):
            await get_instance_details.fn(mock_context)

    @pytest.mark.asyncio
    async def test_get_instance_details_timeout(self, mock_context, watsonx_client, respx_mock):
        """Test instance details with timeout."""
        respx_mock.get("https://test.watsonx.com/api/v2/instance").mock(side_effect=httpx.TimeoutException("Request timed out"))

        with pytest.raises(httpx.TimeoutException):
            await get_instance_details.fn(mock_context)

    @pytest.mark.asyncio
    async def test_get_instance_details_empty_response(self, mock_context, watsonx_client, respx_mock):
        """Test instance details with empty response."""
        respx_mock.get("https://test.watsonx.com/api/v2/instance").mock(return_value=httpx.Response(200, json={}))

        result = await get_instance_details.fn(mock_context)

        # All fields should have default values
        assert result["instance_id"] == "unknown"
        assert result["region"] == "unknown"
        assert result["status"] == "unknown"

    @pytest.mark.asyncio
    async def test_get_instance_details_all_features_disabled(self, mock_context, watsonx_client, respx_mock):
        """Test instance with all optional features disabled."""
        response = {
            "deploymentresponse": {
                "deployment": {
                    "id": "crn:test",
                    "region": "us-south",
                    "status": "active",
                    "enable_public_endpoints": False,
                    "enable_private_endpoints": False,
                    "serverless_spark": False,
                }
            }
        }

        respx_mock.get("https://test.watsonx.com/api/v2/instance").mock(return_value=httpx.Response(200, json=response))

        result = await get_instance_details.fn(mock_context)

        assert result["public_endpoints_enabled"] is False
        assert result["private_endpoints_enabled"] is False
        assert result["serverless_spark_enabled"] is False
