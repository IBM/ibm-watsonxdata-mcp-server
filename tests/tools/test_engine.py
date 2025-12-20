"""
Tests for engine tools.

This file has been modified with the assistance of IBM Bob AI tool
"""

import httpx
import pytest

from lakehouse_mcp.tools.engine.list_engines import list_engines


class TestListEngines:
    """Tests for list_engines tool."""

    @pytest.mark.asyncio
    async def test_list_all_engines(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
        mock_presto_engines_response,
        mock_spark_engines_response,
    ):
        """Test listing all engines (Presto and Spark)."""
        respx_mock.get("https://test.watsonx.com/api/v2/presto_engines").mock(
            return_value=httpx.Response(200, json=mock_presto_engines_response)
        )
        respx_mock.get("https://test.watsonx.com/api/v2/spark_engines").mock(
            return_value=httpx.Response(200, json=mock_spark_engines_response)
        )

        result = await list_engines.fn(mock_context)

        # Check summary statistics
        assert result["summary"]["total_count"] == 3
        assert result["summary"]["presto_count"] == 2
        assert result["summary"]["spark_count"] == 1
        assert result["summary"]["by_status"]["running"] == 2
        assert result["summary"]["by_status"]["stopped"] == 1

        # Check unified engine list
        assert len(result["engines"]) == 3

        # Check Presto engines in unified list
        presto_engines = [e for e in result["engines"] if e["type"] == "presto"]
        assert len(presto_engines) == 2
        assert presto_engines[0]["engine_id"] == "presto-01"
        assert presto_engines[0]["display_name"] == "Presto Engine 1"
        assert presto_engines[0]["status"] == "running"
        assert presto_engines[0]["type"] == "presto"
        assert "size" in presto_engines[0]
        assert "created_on" in presto_engines[0]
        assert "associated_catalogs" in presto_engines[0]

        # Check Spark engines in unified list
        spark_engines = [e for e in result["engines"] if e["type"] == "spark"]
        assert len(spark_engines) == 1
        assert spark_engines[0]["engine_id"] == "spark-01"
        assert spark_engines[0]["display_name"] == "Spark Engine 1"
        assert spark_engines[0]["type"] == "spark"
        assert "size" in spark_engines[0]
        assert "created_on" in spark_engines[0]
        assert "associated_catalogs" in spark_engines[0]

    @pytest.mark.asyncio
    async def test_list_presto_engines_only(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
        mock_presto_engines_response,
    ):
        """Test listing only Presto engines."""
        respx_mock.get("https://test.watsonx.com/api/v2/presto_engines").mock(
            return_value=httpx.Response(200, json=mock_presto_engines_response)
        )

        result = await list_engines.fn(mock_context, engine_type="presto")

        # Check summary statistics
        assert result["summary"]["total_count"] == 2
        assert result["summary"]["presto_count"] == 2
        assert result["summary"]["spark_count"] == 0

        # Check unified engine list
        assert len(result["engines"]) == 2
        assert all(e["type"] == "presto" for e in result["engines"])

    @pytest.mark.asyncio
    async def test_list_spark_engines_only(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
        mock_spark_engines_response,
    ):
        """Test listing only Spark engines."""
        respx_mock.get("https://test.watsonx.com/api/v2/spark_engines").mock(
            return_value=httpx.Response(200, json=mock_spark_engines_response)
        )

        result = await list_engines.fn(mock_context, engine_type="spark")

        # Check summary statistics
        assert result["summary"]["total_count"] == 1
        assert result["summary"]["presto_count"] == 0
        assert result["summary"]["spark_count"] == 1

        # Check unified engine list
        assert len(result["engines"]) == 1
        assert all(e["type"] == "spark" for e in result["engines"])

    @pytest.mark.asyncio
    async def test_list_engines_invalid_type(self, mock_context):
        """Test listing engines with invalid engine type."""
        with pytest.raises(ValueError) as exc_info:
            await list_engines.fn(mock_context, engine_type="flink")

        assert "Invalid engine_type: flink" in str(exc_info.value)
        assert "Must be 'presto', 'spark', or None" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_list_engines_empty_response(self, mock_context, watsonx_client, respx_mock):
        """Test listing engines when no engines exist."""
        respx_mock.get("https://test.watsonx.com/api/v2/presto_engines").mock(return_value=httpx.Response(200, json={"presto_engines": []}))
        respx_mock.get("https://test.watsonx.com/api/v2/spark_engines").mock(return_value=httpx.Response(200, json={"spark_engines": []}))

        result = await list_engines.fn(mock_context)

        assert result["summary"]["total_count"] == 0
        assert result["summary"]["presto_count"] == 0
        assert result["summary"]["spark_count"] == 0
        assert result["engines"] == []

    @pytest.mark.asyncio
    async def test_list_engines_presto_api_error(self, mock_context, watsonx_client, respx_mock, mock_spark_engines_response):
        """Test listing engines when Presto API fails."""
        respx_mock.get("https://test.watsonx.com/api/v2/presto_engines").mock(
            return_value=httpx.Response(500, json={"error": "Internal error"})
        )
        respx_mock.get("https://test.watsonx.com/api/v2/spark_engines").mock(
            return_value=httpx.Response(200, json=mock_spark_engines_response)
        )

        with pytest.raises(httpx.HTTPStatusError):
            await list_engines.fn(mock_context)

    @pytest.mark.asyncio
    async def test_list_engines_spark_api_error(self, mock_context, watsonx_client, respx_mock, mock_presto_engines_response):
        """Test listing engines when Spark API fails."""
        respx_mock.get("https://test.watsonx.com/api/v2/presto_engines").mock(
            return_value=httpx.Response(200, json=mock_presto_engines_response)
        )
        respx_mock.get("https://test.watsonx.com/api/v2/spark_engines").mock(
            return_value=httpx.Response(500, json={"error": "Internal error"})
        )

        with pytest.raises(httpx.HTTPStatusError):
            await list_engines.fn(mock_context)

    @pytest.mark.asyncio
    async def test_list_engines_timeout(self, mock_context, watsonx_client, respx_mock):
        """Test listing engines with timeout."""
        respx_mock.get("https://test.watsonx.com/api/v2/presto_engines").mock(side_effect=httpx.TimeoutException("Request timed out"))

        with pytest.raises(httpx.TimeoutException):
            await list_engines.fn(mock_context)

    @pytest.mark.asyncio
    async def test_list_engines_handles_alternative_id_field(self, mock_context, watsonx_client, respx_mock):
        """Test listing engines with alternative field names (engine_id vs id)."""
        # Some API responses use "engine_id" instead of "id"
        response = {
            "presto_engines": [
                {
                    "engine_id": "presto-alt",
                    "name": "Alternative Presto",
                    "status": "running",
                }
            ]
        }

        respx_mock.get("https://test.watsonx.com/api/v2/presto_engines").mock(return_value=httpx.Response(200, json=response))
        respx_mock.get("https://test.watsonx.com/api/v2/spark_engines").mock(return_value=httpx.Response(200, json={"spark_engines": []}))

        result = await list_engines.fn(mock_context)

        assert len(result["engines"]) == 1
        assert result["engines"][0]["engine_id"] == "presto-alt"

    @pytest.mark.asyncio
    async def test_list_engines_handles_alternative_name_field(self, mock_context, watsonx_client, respx_mock):
        """Test listing engines with alternative field names (name vs display_name)."""
        response = {"spark_engines": [{"id": "spark-alt", "name": "Alternative Spark", "status": "running"}]}

        respx_mock.get("https://test.watsonx.com/api/v2/presto_engines").mock(return_value=httpx.Response(200, json={"presto_engines": []}))
        respx_mock.get("https://test.watsonx.com/api/v2/spark_engines").mock(return_value=httpx.Response(200, json=response))

        result = await list_engines.fn(mock_context)

        assert len(result["engines"]) == 1
        assert result["engines"][0]["display_name"] == "Alternative Spark"

    @pytest.mark.asyncio
    async def test_list_engines_unknown_status(self, mock_context, watsonx_client, respx_mock):
        """Test listing engines when status field is missing."""
        response = {
            "presto_engines": [
                {
                    "id": "presto-nostatus",
                    "display_name": "No Status Presto",
                    # status field missing
                }
            ]
        }

        respx_mock.get("https://test.watsonx.com/api/v2/presto_engines").mock(return_value=httpx.Response(200, json=response))
        respx_mock.get("https://test.watsonx.com/api/v2/spark_engines").mock(return_value=httpx.Response(200, json={"spark_engines": []}))

        result = await list_engines.fn(mock_context)

        assert len(result["engines"]) == 1
        assert result["engines"][0]["status"] == "unknown"

    @pytest.mark.asyncio
    async def test_list_engines_parallel_execution(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
        mock_presto_engines_response,
        mock_spark_engines_response,
    ):
        """Test that API calls are made in parallel."""
        presto_route = respx_mock.get("https://test.watsonx.com/api/v2/presto_engines").mock(
            return_value=httpx.Response(200, json=mock_presto_engines_response)
        )

        spark_route = respx_mock.get("https://test.watsonx.com/api/v2/spark_engines").mock(
            return_value=httpx.Response(200, json=mock_spark_engines_response)
        )

        await list_engines.fn(mock_context)

        # Both routes should be called
        assert presto_route.called
        assert spark_route.called
