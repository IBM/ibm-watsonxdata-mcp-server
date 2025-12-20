"""
Shared test fixtures for ibm-watsonxdata-mcp-server tests.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any
from unittest.mock import Mock

import pytest
import respx
from fastmcp import Context
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator

from lakehouse_mcp.client.watsonx import WatsonXClient
from lakehouse_mcp.config import WatsonXConfig


@pytest.fixture
def mock_env_vars(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """Set up mock environment variables for testing.

    Returns:
        Dictionary of environment variables
    """
    env_vars = {
        "WATSONX_DATA_BASE_URL": "https://test.watsonx.com/api",
        "WATSONX_DATA_API_KEY": "test_api_key_12345",
        "WATSONX_DATA_INSTANCE_ID": "crn:v1:bluemix:public:lakehouse:us-south:a/test123:instance456::",
        "WATSONX_DATA_TIMEOUT_SECONDS": "60",
        "WATSONX_DATA_TLS_INSECURE_SKIP_VERIFY": "false",
        "LOG_LEVEL": "info",
        "OTEL_ENABLED": "false",  # Disable OTel in tests
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    return env_vars


@pytest.fixture
def watsonx_config(mock_env_vars: dict[str, str]) -> WatsonXConfig:
    """Create WatsonX configuration for testing.

    Returns:
        WatsonXConfig instance
    """
    return WatsonXConfig()


@pytest.fixture
def mock_iam_authenticator() -> Mock:
    """Create mock IAM authenticator.

    Returns:
        Mocked IAMAuthenticator
    """
    authenticator = Mock(spec=IAMAuthenticator)
    token_manager = Mock()
    token_manager.get_token.return_value = "mock_access_token_123"
    authenticator.token_manager = token_manager
    return authenticator


@pytest.fixture
async def watsonx_client(watsonx_config: WatsonXConfig, mock_iam_authenticator: Mock) -> WatsonXClient:
    """Create WatsonX client for testing with mocked authenticator.

    Args:
        watsonx_config: WatsonX configuration
        mock_iam_authenticator: Mocked IAM authenticator

    Returns:
        WatsonXClient instance with mocked auth
    """
    client = WatsonXClient(watsonx_config)
    client.authenticator = mock_iam_authenticator
    return client


@pytest.fixture
def mock_context(watsonx_client: WatsonXClient) -> Context:
    """Create mock FastMCP Context for testing tools.

    Args:
        watsonx_client: WatsonX client instance

    Returns:
        Mocked Context with watsonx_client dependency
    """
    context = Mock(spec=Context)
    # FastMCP 2.x stores dependencies on mcp.dependencies, accessed via ctx.fastmcp.dependencies
    mock_fastmcp = Mock()
    mock_fastmcp.dependencies = {"watsonx_client": watsonx_client}
    context.fastmcp = mock_fastmcp
    return context


@pytest.fixture
def respx_mock() -> respx.MockRouter:
    """Create respx mock for HTTP responses.

    Yields:
        respx MockRouter instance
    """
    with respx.mock(assert_all_called=False) as respx_mock:
        yield respx_mock


# Sample API response fixtures


@pytest.fixture
def mock_instance_response() -> dict[str, Any]:
    """Mock instance details API response.

    Returns:
        Sample instance API response
    """
    return {
        "deploymentresponse": {
            "deployment": {
                "id": "crn:v1:bluemix:public:lakehouse:us-south:a/test:inst123::",
                "region": "us-south",
                "cloud_type": "ibm-cloud",
                "account_type": "ENTERPRISE",
                "plan_id": "lakehouse-enterprise",
                "status": "active",
                "instance_status": "deployed",
                "version": "2.0.0",
                "enable_public_endpoints": True,
                "enable_private_endpoints": False,
                "serverless_spark": True,
                "resource_group_crn": "crn:v1:bluemix:public:resource-controller::a/test:::resource-group:rg123",
                "console_url": "https://console.watsonx.test.com",
                "first_time_use": False,
            }
        }
    }


@pytest.fixture
def mock_presto_engines_response() -> dict[str, Any]:
    """Mock Presto engines API response.

    Returns:
        Sample Presto engines API response
    """
    return {
        "presto_engines": [
            {
                "id": "presto-01",
                "display_name": "Presto Engine 1",
                "status": "running",
                "created_on": "2024-01-15T10:30:00Z",
                "size_config": "starter",
                "version": "0.285",
            },
            {
                "id": "presto-02",
                "display_name": "Presto Engine 2",
                "status": "stopped",
                "created_on": "2024-01-16T14:20:00Z",
                "size_config": "small",
                "version": "0.285",
            },
        ]
    }


@pytest.fixture
def mock_spark_engines_response() -> dict[str, Any]:
    """Mock Spark engines API response.

    Returns:
        Sample Spark engines API response
    """
    return {
        "spark_engines": [
            {
                "id": "spark-01",
                "display_name": "Spark Engine 1",
                "status": "running",
                "created_on": "2024-01-17T09:00:00Z",
                "spark_version": "3.3.2",
            }
        ]
    }


@pytest.fixture
def mock_schemas_response() -> dict[str, Any]:
    """Mock list schemas API response.

    Returns:
        Sample schemas API response
    """
    return {
        "schemas": [
            {"name": "default"},
            {"name": "analytics"},
            {"name": "reporting"},
        ]
    }


@pytest.fixture
def mock_tables_response() -> dict[str, Any]:
    """Mock list tables API response.

    Returns:
        Sample tables API response (API returns simple string array)
    """
    return {"tables": ["customers", "orders", "customer_view"]}


@pytest.fixture
def mock_describe_table_response() -> dict[str, Any]:
    """Mock describe table API response.

    Returns:
        Sample describe table API response
    """
    return {
        "columns": [
            {"name": "id", "type": "bigint", "nullable": False, "comment": "Primary key"},
            {
                "name": "name",
                "type": "varchar",
                "nullable": False,
                "comment": "Customer name",
            },
            {"name": "email", "type": "varchar", "nullable": True, "comment": None},
            {
                "name": "created_at",
                "type": "timestamp",
                "nullable": False,
                "comment": "Creation timestamp",
            },
        ],
        "partitions": ["created_at"],
        "primary_keys": [],
        "properties": {},
        "table_type": "MANAGED_TABLE",
    }


@pytest.fixture
def mock_query_submit_response() -> dict[str, Any]:
    """Mock query submit API response.

    Returns:
        Sample query submit response
    """
    return {
        "id": "query-123",
        "stats": {"state": "RUNNING"},
        "nextUri": "https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01",
    }


@pytest.fixture
def mock_query_running_response() -> dict[str, Any]:
    """Mock query running API response.

    Returns:
        Sample query running response
    """
    return {
        "id": "query-123",
        "stats": {"state": "RUNNING"},
        "nextUri": "https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01",
    }


@pytest.fixture
def mock_query_complete_response() -> dict[str, Any]:
    """Mock query complete API response.

    Returns:
        Sample query complete response
    """
    return {
        "id": "query-123",
        "stats": {"state": "FINISHED"},
        "columns": [
            {"name": "id", "type": "bigint"},
            {"name": "name", "type": "varchar"},
            {"name": "total", "type": "decimal"},
        ],
        "data": [
            [1, "Customer A", 1500.50],
            [2, "Customer B", 2300.75],
            [3, "Customer C", 890.25],
        ],
    }
