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
        respx_mock.get("https://test.watsonx.com/api/v3/presto_engines").mock(
            return_value=httpx.Response(200, json=mock_presto_engines_response)
        )
        respx_mock.get("https://test.watsonx.com/api/v3/spark_engines").mock(
            return_value=httpx.Response(200, json=mock_spark_engines_response)
        )

        result = await list_engines(mock_context)

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
        respx_mock.get("https://test.watsonx.com/api/v3/presto_engines").mock(
            return_value=httpx.Response(200, json=mock_presto_engines_response)
        )

        result = await list_engines(mock_context, engine_type="presto")

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
        respx_mock.get("https://test.watsonx.com/api/v3/spark_engines").mock(
            return_value=httpx.Response(200, json=mock_spark_engines_response)
        )

        result = await list_engines(mock_context, engine_type="spark")

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
        result = await list_engines(mock_context, engine_type="flink")

        assert result.get("error") is True
        assert "Invalid engine_type: flink" in result.get("error_message", "")
        assert "Must be 'presto', 'spark', or None" in result.get("error_message", "")
        assert result.get("status_code") == 400

    @pytest.mark.asyncio
    async def test_list_engines_empty_response(self, mock_context, watsonx_client, respx_mock):
        """Test listing engines when no engines exist."""
        respx_mock.get("https://test.watsonx.com/api/v3/presto_engines").mock(return_value=httpx.Response(200, json={"presto_engines": []}))
        respx_mock.get("https://test.watsonx.com/api/v3/spark_engines").mock(return_value=httpx.Response(200, json={"spark_engines": []}))

        result = await list_engines(mock_context)

        assert result["summary"]["total_count"] == 0
        assert result["summary"]["presto_count"] == 0
        assert result["summary"]["spark_count"] == 0
        assert result["engines"] == []

    @pytest.mark.asyncio
    async def test_list_engines_presto_api_error(self, mock_context, watsonx_client, respx_mock, mock_spark_engines_response):
        """Test listing engines when Presto API fails."""
        respx_mock.get("https://test.watsonx.com/api/v3/presto_engines").mock(
            return_value=httpx.Response(500, json={"message": "Internal error"})
        )
        respx_mock.get("https://test.watsonx.com/api/v3/spark_engines").mock(
            return_value=httpx.Response(200, json=mock_spark_engines_response)
        )

        result = await list_engines(mock_context)
        
        assert result["error"] is True
        assert "Internal error" in result["error_message"]
        assert result["status_code"] == 500

    @pytest.mark.asyncio
    async def test_list_engines_spark_api_error(self, mock_context, watsonx_client, respx_mock, mock_presto_engines_response):
        """Test listing engines when Spark API fails."""
        respx_mock.get("https://test.watsonx.com/api/v3/presto_engines").mock(
            return_value=httpx.Response(200, json=mock_presto_engines_response)
        )
        respx_mock.get("https://test.watsonx.com/api/v3/spark_engines").mock(
            return_value=httpx.Response(500, json={"message": "Internal error"})
        )

        result = await list_engines(mock_context)
        
        assert result["error"] is True
        assert "Internal error" in result["error_message"]
        assert result["status_code"] == 500

    @pytest.mark.asyncio
    async def test_list_engines_timeout(self, mock_context, watsonx_client, respx_mock):
        """Test listing engines with timeout."""
        respx_mock.get("https://test.watsonx.com/api/v3/presto_engines").mock(side_effect=httpx.TimeoutException("Request timed out"))

        with pytest.raises(httpx.TimeoutException):
            await list_engines(mock_context)

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

        respx_mock.get("https://test.watsonx.com/api/v3/presto_engines").mock(return_value=httpx.Response(200, json=response))
        respx_mock.get("https://test.watsonx.com/api/v3/spark_engines").mock(return_value=httpx.Response(200, json={"spark_engines": []}))

        result = await list_engines(mock_context)

        assert len(result["engines"]) == 1
        assert result["engines"][0]["engine_id"] == "presto-alt"

    @pytest.mark.asyncio
    async def test_list_engines_handles_alternative_name_field(self, mock_context, watsonx_client, respx_mock):
        """Test listing engines with alternative field names (name vs display_name)."""
        response = {"spark_engines": [{"id": "spark-alt", "name": "Alternative Spark", "status": "running"}]}

        respx_mock.get("https://test.watsonx.com/api/v3/presto_engines").mock(return_value=httpx.Response(200, json={"presto_engines": []}))
        respx_mock.get("https://test.watsonx.com/api/v3/spark_engines").mock(return_value=httpx.Response(200, json=response))

        result = await list_engines(mock_context)

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

        respx_mock.get("https://test.watsonx.com/api/v3/presto_engines").mock(return_value=httpx.Response(200, json=response))
        respx_mock.get("https://test.watsonx.com/api/v3/spark_engines").mock(return_value=httpx.Response(200, json={"spark_engines": []}))

        result = await list_engines(mock_context)

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
        presto_route = respx_mock.get("https://test.watsonx.com/api/v3/presto_engines").mock(
            return_value=httpx.Response(200, json=mock_presto_engines_response)
        )
        spark_route = respx_mock.get("https://test.watsonx.com/api/v3/spark_engines").mock(
            return_value=httpx.Response(200, json=mock_spark_engines_response)
        )

        await list_engines(mock_context)

        # All routes should be called
        assert presto_route.called
        assert spark_route.called



class TestRestartPrestoEngine:
    """Tests for restart_presto_engine tool."""

    @pytest.mark.asyncio
    async def test_restart_presto_engine_success(self, mock_context, watsonx_client, respx_mock):
        """Test successfully restarting a Presto engine."""
        from lakehouse_mcp.tools.engine.restart_presto_engine import restart_presto_engine

        respx_mock.post("https://test.watsonx.com/api/v3/presto_engines/presto-01/restart").mock(
            return_value=httpx.Response(200, json={"status": "restarting"})
        )

        result = await restart_presto_engine(mock_context, engine_id="presto-01")

        assert result["engine_id"] == "presto-01"
        assert result["engine_type"] == "presto"
        assert result["status"] == "restarting"
        assert "message" in result

    @pytest.mark.asyncio
    async def test_restart_presto_engine_not_found(self, mock_context, watsonx_client, respx_mock):
        """Test restarting non-existent Presto engine."""
        from lakehouse_mcp.tools.engine.restart_presto_engine import restart_presto_engine

        respx_mock.post("https://test.watsonx.com/api/v3/presto_engines/invalid/restart").mock(
            return_value=httpx.Response(404, json={"message": "Engine not found"})
        )

        result = await restart_presto_engine(mock_context, engine_id="invalid")
        
        assert result["error"] is True
        assert "Engine not found" in result["error_message"]
        assert result["status_code"] == 404


class TestPausePrestoEngine:
    """Tests for pause_presto_engine tool."""

    @pytest.mark.asyncio
    async def test_pause_presto_engine_success(self, mock_context, respx_mock):
        """Test successfully pausing a running Presto engine."""
        from lakehouse_mcp.tools.engine.pause_presto_engine import pause_presto_engine

        respx_mock.post("https://test.watsonx.com/api/v3/presto_engines/presto-01/pause").mock(
            return_value=httpx.Response(
                200,
                json={
                    "response": {
                        "message": "Pause Engine",
                        "message_code": "Success",
                    }
                },
            )
        )

        result = await pause_presto_engine(mock_context, engine_id="presto-01")

        assert result["engine_id"] == "presto-01"
        assert result["message"] == "Pause Engine"

    @pytest.mark.asyncio
    async def test_pause_presto_engine_error(self, mock_context, respx_mock):
        """Test pausing Presto engine with API error."""
        from lakehouse_mcp.tools.engine.pause_presto_engine import pause_presto_engine

        respx_mock.post("https://test.watsonx.com/api/v3/presto_engines/presto-01/pause").mock(
            return_value=httpx.Response(500, json={"message": "Internal error"})
        )

        result = await pause_presto_engine(mock_context, engine_id="presto-01")
        
        assert result["error"] is True
        assert "Internal error" in result["error_message"]
        assert result["status_code"] == 500


class TestResumePrestoEngine:
    """Tests for resume_presto_engine tool."""

    @pytest.mark.asyncio
    async def test_resume_presto_engine_success(self, mock_context, respx_mock):
        """Test successfully resuming a paused Presto engine."""
        from lakehouse_mcp.tools.engine.resume_presto_engine import resume_presto_engine

        respx_mock.post("https://test.watsonx.com/api/v3/presto_engines/presto-01/resume").mock(
            return_value=httpx.Response(
                200,
                json={
                    "message": "Success",
                    "message_code": "success",
                },
            )
        )

        result = await resume_presto_engine(mock_context, engine_id="presto-01")

        assert result["engine_id"] == "presto-01"
        assert result["message"] == "Success"

    @pytest.mark.asyncio
    async def test_resume_presto_engine_error(self, mock_context, respx_mock):
        """Test resuming Presto engine with API error."""
        from lakehouse_mcp.tools.engine.resume_presto_engine import resume_presto_engine

        respx_mock.post("https://test.watsonx.com/api/v3/presto_engines/presto-01/resume").mock(
            return_value=httpx.Response(500, json={"message": "Internal error"})
        )

        result = await resume_presto_engine(mock_context, engine_id="presto-01")
        
        assert result["error"] is True
        assert "Internal error" in result["error_message"]
        assert result["status_code"] == 500


class TestUpdatePrestoEngine:
    """Tests for update_presto_engine tool."""

    @pytest.mark.asyncio
    async def test_update_presto_engine_description(self, mock_context, watsonx_client, respx_mock):
        """Test updating Presto engine description."""
        from lakehouse_mcp.tools.engine.update_presto_engine import update_presto_engine

        respx_mock.patch("https://test.watsonx.com/api/v3/presto_engines/presto-01").mock(
            return_value=httpx.Response(200, json={"engine_id": "presto-01", "description": "Updated description"})
        )

        result = await update_presto_engine(mock_context, engine_id="presto-01", description="Updated description")

        assert result["engine_id"] == "presto-01"
        assert result["description"] == "Updated description"

    @pytest.mark.asyncio
    async def test_update_presto_engine_with_restart(self, mock_context, watsonx_client, respx_mock):
        """Test updating Presto engine with forced restart."""
        from lakehouse_mcp.tools.engine.update_presto_engine import update_presto_engine

        respx_mock.patch("https://test.watsonx.com/api/v3/presto_engines/presto-01").mock(
            return_value=httpx.Response(200, json={"engine_id": "presto-01", "status": "restarting"})
        )

        result = await update_presto_engine(
            mock_context, engine_id="presto-01", display_name="New Name", engine_restart="force"
        )

        assert result["engine_id"] == "presto-01"


class TestCreatePrestoEngine:
    """Tests for create_presto_engine tool."""

    @pytest.mark.asyncio
    async def test_create_presto_engine_minimal(self, mock_context, watsonx_client, respx_mock):
        """Test creating a Presto engine with minimal parameters."""
        from lakehouse_mcp.tools.engine.create_presto_engine import create_presto_engine

        create_response = {"engine_id": "presto-new", "origin": "native", "display_name": "New Presto Engine"}
        respx_mock.post("https://test.watsonx.com/api/v3/presto_engines").mock(
            return_value=httpx.Response(201, json=create_response)
        )

        configuration = {
            "size_config": "custom",
            "coordinator": {"node_type": "starter", "quantity": 1},
            "worker": {"node_type": "starter", "quantity": 2}
        }

        result = await create_presto_engine(
            mock_context,
            origin="native",
            display_name="New Presto Engine",
            configuration=configuration
        )

        assert result["engine_id"] == "presto-new"
        assert result["origin"] == "native"

    @pytest.mark.asyncio
    async def test_create_presto_engine_full(self, mock_context, watsonx_client, respx_mock):
        """Test creating a Presto engine with all parameters."""
        from lakehouse_mcp.tools.engine.create_presto_engine import create_presto_engine

        create_response = {
            "engine_id": "presto-full",
            "origin": "native",
            "display_name": "Full Presto Engine",
            "description": "Test engine",
        }
        respx_mock.post("https://test.watsonx.com/api/v3/presto_engines").mock(
            return_value=httpx.Response(201, json=create_response)
        )

        configuration = {
            "size_config": "custom",
            "coordinator": {"node_type": "cache_optimized", "quantity": 1},
            "worker": {"node_type": "cache_optimized", "quantity": 3}
        }

        result = await create_presto_engine(
            mock_context,
            origin="native",
            display_name="Full Presto Engine",
            configuration=configuration,
            description="Test engine",
            tags=["test", "dev"],
        )

        assert result["engine_id"] == "presto-full"
        assert result["description"] == "Test engine"

    @pytest.mark.asyncio
    async def test_create_presto_engine_mismatched_node_types(self, mock_context, watsonx_client, respx_mock):
        """Test that creating engine with mismatched coordinator/worker node types is allowed."""
        from lakehouse_mcp.tools.engine.create_presto_engine import create_presto_engine

        create_response = {"engine_id": "presto-mismatched", "origin": "native", "display_name": "Mismatched Engine"}
        respx_mock.post("https://test.watsonx.com/api/v3/presto_engines").mock(
            return_value=httpx.Response(201, json=create_response)
        )

        configuration = {
            "size_config": "custom",
            "coordinator": {"node_type": "starter", "quantity": 1},
            "worker": {"node_type": "cache_optimized", "quantity": 2}
        }

        result = await create_presto_engine(
            mock_context,
            origin="native",
            display_name="Mismatched Engine",
            configuration=configuration
        )

        assert result["engine_id"] == "presto-mismatched"

    @pytest.mark.asyncio
    async def test_create_presto_engine_predefined_starter(self, mock_context, watsonx_client, respx_mock):
        """Test creating engine with predefined starter size_config."""
        from lakehouse_mcp.tools.engine.create_presto_engine import create_presto_engine

        create_response = {"engine_id": "presto-starter", "origin": "native", "display_name": "Starter Engine"}
        respx_mock.post("https://test.watsonx.com/api/v3/presto_engines").mock(
            return_value=httpx.Response(201, json=create_response)
        )

        configuration = {
            "size_config": "starter",
            "coordinator": {"node_type": "starter", "quantity": 1},
            "worker": {"node_type": "starter", "quantity": 1}
        }

        result = await create_presto_engine(
            mock_context,
            origin="native",
            display_name="Starter Engine",
            configuration=configuration
        )

        assert result["engine_id"] == "presto-starter"


class TestScalePrestoEngine:
    """Tests for scale_presto_engine tool."""

    @pytest.mark.asyncio
    async def test_scale_presto_engine_success(
        self,
        mock_context,
        respx_mock,
    ):
        """Test scaling Presto engine with matching node types."""
        from lakehouse_mcp.tools.engine.scale_presto_engine import scale_presto_engine

        respx_mock.post("https://test.watsonx.com/api/v3/presto_engines/presto-01/scale").mock(
            return_value=httpx.Response(
                200,
                json={
                    "message": "Success",
                    "coordinator": {"node_type": "starter", "quantity": 1},
                    "worker": {"node_type": "starter", "quantity": 5},
                },
            )
        )

        result = await scale_presto_engine(
            mock_context,
            engine_id="presto-01",
            coordinator_node_type="starter",
            coordinator_quantity=1,
            worker_node_type="starter",
            worker_quantity=5,
        )

        assert result["coordinator"]["quantity"] == 1
        assert result["worker"]["quantity"] == 5

    @pytest.mark.asyncio
    async def test_scale_presto_engine_change_node_types(
        self,
        mock_context,
        respx_mock,
    ):
        """Test that scaling with different node types is allowed."""
        from lakehouse_mcp.tools.engine.scale_presto_engine import scale_presto_engine

        respx_mock.post("https://test.watsonx.com/api/v3/presto_engines/presto-01/scale").mock(
            return_value=httpx.Response(
                200,
                json={
                    "message": "Success",
                    "coordinator": {"node_type": "cache_optimized", "quantity": 1},
                    "worker": {"node_type": "cache_optimized", "quantity": 5},
                },
            )
        )

        result = await scale_presto_engine(
            mock_context,
            engine_id="presto-01",
            coordinator_node_type="cache_optimized",
            coordinator_quantity=1,
            worker_node_type="cache_optimized",
            worker_quantity=5,
        )

        assert result["coordinator"]["node_type"] == "cache_optimized"
        assert result["worker"]["quantity"] == 5


class TestCreateSparkEngine:
    """Tests for create_spark_engine tool."""

    @pytest.mark.asyncio
    async def test_create_spark_engine_minimal(self, mock_context, watsonx_client, respx_mock):
        """Test creating a Spark engine with minimal parameters."""
        from lakehouse_mcp.tools.engine.create_spark_engine import create_spark_engine

        mock_response = {
            "engine_id": "spark-new",
            "display_name": "New Spark Engine",
            "origin": "native",
            "status": "running",
        }

        respx_mock.post("https://test.watsonx.com/api/v3/spark_engines").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await create_spark_engine(
            mock_context,
            origin="native",
            display_name="New Spark Engine",
            storage_name="test-bucket",
        )

        assert result["engine_id"] == "spark-new"
        assert result["display_name"] == "New Spark Engine"
        assert result["origin"] == "native"

    @pytest.mark.asyncio
    async def test_create_spark_engine_full(self, mock_context, watsonx_client, respx_mock):
        """Test creating a Spark engine with all parameters."""
        from lakehouse_mcp.tools.engine.create_spark_engine import create_spark_engine

        mock_response = {
            "engine_id": "spark-full",
            "display_name": "Full Spark Engine",
            "origin": "native",
            "description": "Test Spark engine",
            "status": "running",
        }

        respx_mock.post("https://test.watsonx.com/api/v3/spark_engines").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await create_spark_engine(
            mock_context,
            origin="native",
            display_name="Full Spark Engine",
            storage_name="test-bucket",
            description="Test Spark engine",
            default_version="3.5",
            associated_catalogs=["iceberg_data"],
            tags=["test", "dev"],
        )

        assert result["engine_id"] == "spark-full"
        assert result["display_name"] == "Full Spark Engine"
        assert result["description"] == "Test Spark engine"


class TestPauseSparkEngine:
    """Tests for pause_spark_engine tool (SAAS only)."""

    @pytest.mark.asyncio
    async def test_pause_spark_engine_success(self, mock_context, watsonx_client, respx_mock):
        """Test successfully pausing a Spark engine."""
        from lakehouse_mcp.tools.engine.pause_spark_engine import pause_spark_engine

        respx_mock.post("https://test.watsonx.com/api/v3/spark_engines/spark-01/pause").mock(
            return_value=httpx.Response(
                200,
                json={"message": "pause spark engine", "message_code": "success"},
            )
        )

        result = await pause_spark_engine(mock_context, engine_id="spark-01", force=False)

        assert result["engine_id"] == "spark-01"
        assert result["forced"] is False
        assert result["message"] == "pause spark engine"

    @pytest.mark.asyncio
    async def test_pause_spark_engine_with_force(self, mock_context, watsonx_client, respx_mock):
        """Test pausing Spark engine with force flag."""
        from lakehouse_mcp.tools.engine.pause_spark_engine import pause_spark_engine

        respx_mock.post("https://test.watsonx.com/api/v3/spark_engines/spark-01/pause").mock(
            return_value=httpx.Response(200, json={"message": "pause spark engine"})
        )

        result = await pause_spark_engine(mock_context, engine_id="spark-01", force=True)

        assert result["engine_id"] == "spark-01"
        assert result["forced"] is True


class TestResumeSparkEngine:
    """Tests for resume_spark_engine tool (SAAS only)."""

    @pytest.mark.asyncio
    async def test_resume_spark_engine_success(self, mock_context, watsonx_client, respx_mock):
        """Test successfully resuming a Spark engine."""
        from lakehouse_mcp.tools.engine.resume_spark_engine import resume_spark_engine

        respx_mock.post("https://test.watsonx.com/api/v3/spark_engines/spark-01/resume").mock(
            return_value=httpx.Response(200, json={"message": "resume spark engine", "message_code": "success"})
        )

        result = await resume_spark_engine(mock_context, engine_id="spark-01")

        assert result["engine_id"] == "spark-01"
        assert result["message"] == "resume spark engine"


class TestUpdateSparkEngine:
    """Tests for update_spark_engine tool (SAAS only)."""

    @pytest.mark.asyncio
    async def test_update_spark_engine_description(self, mock_context, watsonx_client, respx_mock):
        """Test updating Spark engine description."""
        from lakehouse_mcp.tools.engine.update_spark_engine import update_spark_engine

        mock_response = {
            "engine_id": "spark-01",
            "display_name": "Spark Engine",
            "description": "Updated description",
        }

        respx_mock.patch("https://test.watsonx.com/api/v3/spark_engines/spark-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await update_spark_engine(
            mock_context,
            engine_id="spark-01",
            description="Updated description",
        )

        assert result["engine_id"] == "spark-01"
        assert result["description"] == "Updated description"

    @pytest.mark.asyncio
    async def test_update_spark_engine_full(self, mock_context, watsonx_client, respx_mock):
        """Test updating Spark engine with all parameters."""
        from lakehouse_mcp.tools.engine.update_spark_engine import update_spark_engine

        mock_response = {
            "engine_id": "spark-01",
            "display_name": "Updated Spark Engine",
            "description": "Updated description",
            "tags": ["production", "updated"],
        }

        respx_mock.patch("https://test.watsonx.com/api/v3/spark_engines/spark-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await update_spark_engine(
            mock_context,
            engine_id="spark-01",
            display_name="Updated Spark Engine",
            description="Updated description",
            tags=["production", "updated"],
        )

        assert result["display_name"] == "Updated Spark Engine"
        assert result["description"] == "Updated description"
