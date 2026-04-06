"""
Tests for Spark application tools.

This file has been modified with the assistance of IBM Bob AI tool
"""

import httpx
import pytest

from lakehouse_mcp.tools.spark_applications.get_spark_application_status import get_spark_application_status
from lakehouse_mcp.tools.spark_applications.list_spark_applications import list_spark_applications
from lakehouse_mcp.tools.spark_applications.stop_spark_application import stop_spark_application
from lakehouse_mcp.tools.spark_applications.submit_spark_application import submit_spark_application


class TestSubmitSparkApplication:
    """Tests for submit_spark_application tool."""

    @pytest.mark.asyncio
    async def test_submit_basic_application(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test submitting a basic Spark application."""
        mock_response = {
            "application_id": "app-123",
            "state": "submitted",
            "submission_time": "2024-01-01T00:00:00Z",
        }

        respx_mock.post("https://test.watsonx.com/api/v3/spark_engines/spark-01/applications").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await submit_spark_application(
            mock_context,
            engine_id="spark-01",
            application="s3://bucket/my-app.jar",
            name="My Spark App",
        )

        assert result["application_id"] == "app-123"
        assert result["state"] == "submitted"
        assert result["submission_time"] == "2024-01-01T00:00:00Z"

    @pytest.mark.asyncio
    async def test_submit_application_with_config(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test submitting a Spark application with configuration."""
        mock_response = {
            "application_id": "app-456",
            "state": "submitted",
            "submission_time": "2024-01-01T00:00:00Z",
        }

        respx_mock.post("https://test.watsonx.com/api/v3/spark_engines/spark-01/applications").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await submit_spark_application(
            mock_context,
            engine_id="spark-01",
            application="s3://bucket/my-app.py",
            name="Python Spark App",
            arguments=["--input", "s3://data/input", "--output", "s3://data/output"],
            conf={
                "spark.executor.memory": "4g",
                "spark.executor.cores": "2",
            },
            env={
                "SPARK_ENV": "production",
            },
        )

        assert result["application_id"] == "app-456"
        assert result["state"] == "submitted"

    @pytest.mark.asyncio
    async def test_submit_application_with_volumes(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test submitting a Spark application with volume mounts."""
        mock_response = {
            "application_id": "app-789",
            "state": "submitted",
        }

        respx_mock.post("https://test.watsonx.com/api/v3/spark_engines/spark-01/applications").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await submit_spark_application(
            mock_context,
            engine_id="spark-01",
            application="local:///app/my-app.jar",
            volumes=[
                {
                    "name": "data-volume",
                    "mount_path": "/data",
                    "source_path": "s3://bucket/data",
                }
            ],
        )

        assert result["application_id"] == "app-789"


class TestListSparkApplications:
    """Tests for list_spark_applications tool."""

    @pytest.mark.asyncio
    async def test_list_all_applications(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test listing all Spark applications."""
        mock_response = {
            "applications": [
                {
                    "application_id": "app-123",
                    "name": "App 1",
                    "state": "running",
                    "start_time": "2024-01-01T00:00:00Z",
                },
                {
                    "application_id": "app-456",
                    "name": "App 2",
                    "state": "finished",
                    "start_time": "2024-01-01T01:00:00Z",
                    "end_time": "2024-01-01T02:00:00Z",
                },
            ]
        }

        respx_mock.get("https://test.watsonx.com/api/v3/spark_engines/spark-01/applications").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await list_spark_applications(
            mock_context,
            engine_id="spark-01",
        )

        assert len(result["applications"]) == 2
        assert result["applications"][0]["application_id"] == "app-123"
        assert result["applications"][0]["state"] == "running"
        assert result["applications"][1]["application_id"] == "app-456"
        assert result["applications"][1]["state"] == "finished"

    @pytest.mark.asyncio
    async def test_list_applications_filtered_by_state(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test listing Spark applications filtered by state."""
        mock_response = {
            "applications": [
                {
                    "application_id": "app-123",
                    "name": "Running App",
                    "state": "running",
                    "start_time": "2024-01-01T00:00:00Z",
                },
            ]
        }

        respx_mock.get("https://test.watsonx.com/api/v3/spark_engines/spark-01/applications").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await list_spark_applications(
            mock_context,
            engine_id="spark-01",
            state=["running"],
        )

        assert len(result["applications"]) == 1
        assert result["applications"][0]["state"] == "running"

    @pytest.mark.asyncio
    async def test_list_applications_empty(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test listing Spark applications when none exist."""
        mock_response = {"applications": []}

        respx_mock.get("https://test.watsonx.com/api/v3/spark_engines/spark-01/applications").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await list_spark_applications(
            mock_context,
            engine_id="spark-01",
        )

        assert len(result["applications"]) == 0


class TestGetSparkApplicationStatus:
    """Tests for get_spark_application_status tool."""

    @pytest.mark.asyncio
    async def test_get_running_application_status(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test getting status of a running Spark application."""
        mock_response = {
            "application_id": "app-123",
            "name": "My Spark App",
            "state": "running",
            "start_time": "2024-01-01T00:00:00Z",
            "spark_version": "3.3.0",
            "application_details": {
                "application": "s3://bucket/my-app.jar",
                "conf": {
                    "spark.executor.memory": "4g",
                },
            },
        }

        respx_mock.get("https://test.watsonx.com/api/v3/spark_engines/spark-01/applications/app-123").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await get_spark_application_status(
            mock_context,
            engine_id="spark-01",
            application_id="app-123",
        )

        assert result["application_id"] == "app-123"
        assert result["name"] == "My Spark App"
        assert result["state"] == "running"
        assert result["spark_version"] == "3.3.0"
        assert "start_time" in result
        assert "application_details" in result

    @pytest.mark.asyncio
    async def test_get_finished_application_status(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test getting status of a finished Spark application."""
        mock_response = {
            "application_id": "app-456",
            "name": "Completed App",
            "state": "finished",
            "start_time": "2024-01-01T00:00:00Z",
            "end_time": "2024-01-01T01:00:00Z",
            "spark_version": "3.3.0",
        }

        respx_mock.get("https://test.watsonx.com/api/v3/spark_engines/spark-01/applications/app-456").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await get_spark_application_status(
            mock_context,
            engine_id="spark-01",
            application_id="app-456",
        )

        assert result["application_id"] == "app-456"
        assert result["state"] == "finished"
        assert "end_time" in result

    @pytest.mark.asyncio
    async def test_get_failed_application_status(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test getting status of a failed Spark application."""
        mock_response = {
            "application_id": "app-789",
            "name": "Failed App",
            "state": "failed",
            "start_time": "2024-01-01T00:00:00Z",
            "end_time": "2024-01-01T00:30:00Z",
            "error_message": "Application failed due to OOM",
        }

        respx_mock.get("https://test.watsonx.com/api/v3/spark_engines/spark-01/applications/app-789").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await get_spark_application_status(
            mock_context,
            engine_id="spark-01",
            application_id="app-789",
        )

        assert result["application_id"] == "app-789"
        assert result["state"] == "failed"
        assert "error_message" in result


class TestStopSparkApplication:
    """Tests for stop_spark_application tool."""

    @pytest.mark.asyncio
    async def test_stop_application_success(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test successfully stopping a Spark application."""
        mock_response = {
            "message": "Application stopped successfully",
        }

        respx_mock.delete("https://test.watsonx.com/api/v3/spark_engines/spark-01/applications/app-123").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await stop_spark_application(
            mock_context,
            engine_id="spark-01",
            application_id="app-123",
        )

        assert "message" in result

    @pytest.mark.asyncio
    async def test_stop_running_application(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test stopping a running Spark application."""
        mock_response = {
            "message": "Application stopped and removed",
        }

        respx_mock.delete("https://test.watsonx.com/api/v3/spark_engines/spark-01/applications/app-running").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await stop_spark_application(
            mock_context,
            engine_id="spark-01",
            application_id="app-running",
        )

        assert "message" in result

    @pytest.mark.asyncio
    async def test_stop_nonexistent_application(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test stopping a non-existent Spark application."""
        respx_mock.delete("https://test.watsonx.com/api/v3/spark_engines/spark-01/applications/app-nonexistent").mock(
            return_value=httpx.Response(404, json={"message": "Application not found"})
        )

        result = await stop_spark_application(
            mock_context,
            engine_id="spark-01",
            application_id="app-nonexistent",
        )
        
        assert result["error"] is True
        assert "Application not found" in result["error_message"]
        assert result["status_code"] == 404
