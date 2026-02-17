"""
Tests for data ingestion tools.

This file has been modified with the assistance of IBM Bob AI tool
"""

import httpx
import pytest

from lakehouse_mcp.tools.ingestion.create_ingestion_job import create_ingestion_job
from lakehouse_mcp.tools.ingestion.delete_ingestion_job import delete_ingestion_job
from lakehouse_mcp.tools.ingestion.get_ingestion_job import get_ingestion_job
from lakehouse_mcp.tools.ingestion.list_ingestion_jobs import list_ingestion_jobs


class TestCreateIngestionJob:
    """Tests for create_ingestion_job tool."""

    @pytest.mark.asyncio
    async def test_create_basic_ingestion_job(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test creating a basic ingestion job."""
        mock_response = {
            "job_id": "job-123",
            "status": "starting",
            "start_timestamp": "1770411298720316572",
        }

        respx_mock.post("https://test.watsonx.com/api/v3/lhingestion/api/v1/ingestion/jobs").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await create_ingestion_job.fn(
            mock_context,
            job_id="job-123",
            catalog="iceberg_data",
            schema="default",
            table="my_table",
            file_paths="s3://bucket/data/input.csv",
            file_type="csv",
        )

        assert result["job_id"] == "job-123"
        assert result["status"] == "starting"
        assert "start_timestamp" in result

    @pytest.mark.asyncio
    async def test_create_ingestion_job_with_csv_config(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test creating an ingestion job with CSV configuration."""
        mock_response = {
            "job_id": "job-456",
            "status": "starting",
            "start_timestamp": "1770411298720316572",
        }

        respx_mock.post("https://test.watsonx.com/api/v3/lhingestion/api/v1/ingestion/jobs").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await create_ingestion_job.fn(
            mock_context,
            job_id="job-456",
            catalog="iceberg_data",
            schema="default",
            table="csv_table",
            file_paths="s3://bucket/data/input.csv",
            file_type="csv",
            field_delimiter=",",
            header=True,
        )

        assert result["job_id"] == "job-456"
        assert result["status"] == "starting"

    @pytest.mark.asyncio
    async def test_create_ingestion_job_with_parquet(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test creating an ingestion job with parquet files."""
        mock_response = {
            "job_id": "job-789",
            "status": "starting",
            "start_timestamp": "1770411298720316572",
        }

        respx_mock.post("https://test.watsonx.com/api/v3/lhingestion/api/v1/ingestion/jobs").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await create_ingestion_job.fn(
            mock_context,
            job_id="job-789",
            catalog="iceberg_data",
            schema="default",
            table="partitioned_table",
            file_paths="s3://bucket/data/parquet/*.parquet",
            file_type="parquet",
        )

        assert result["job_id"] == "job-789"
        assert result["status"] == "starting"

    @pytest.mark.asyncio
    async def test_create_ingestion_job_with_spark_config(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test creating an ingestion job with Spark executor configuration."""
        mock_response = {
            "job_id": "job-abc",
            "status": "starting",
            "start_timestamp": "1770411298720316572",
        }

        respx_mock.post("https://test.watsonx.com/api/v3/lhingestion/api/v1/ingestion/jobs").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await create_ingestion_job.fn(
            mock_context,
            job_id="job-abc",
            catalog="iceberg_data",
            schema="default",
            table="large_table",
            file_paths="s3://bucket/data/large-dataset.json",
            file_type="json",
            engine_id="spark473",
            executor_memory="4G",
            executor_cores=2,
        )

        assert result["job_id"] == "job-abc"
        assert result["status"] == "starting"

    @pytest.mark.asyncio
    async def test_create_ingestion_job_with_write_mode(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test creating an ingestion job with overwrite mode."""
        mock_response = {
            "job_id": "job-def",
            "status": "starting",
            "start_timestamp": "1770411298720316572",
        }

        respx_mock.post("https://test.watsonx.com/api/v3/lhingestion/api/v1/ingestion/jobs").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await create_ingestion_job.fn(
            mock_context,
            job_id="job-def",
            catalog="iceberg_data",
            schema="default",
            table="typed_table",
            file_paths="s3://bucket/data/input.csv",
            file_type="csv",
            write_mode="overwrite",
        )

        assert result["job_id"] == "job-def"
        assert result["status"] == "starting"


class TestListIngestionJobs:
    """Tests for list_ingestion_jobs tool."""

    @pytest.mark.asyncio
    async def test_list_all_jobs(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test listing all ingestion jobs."""
        mock_response = {
            "ingestion_jobs": [
                {
                    "job_id": "job-123",
                    "status": "running",
                    "source_data_files": "s3://bucket/data/input1.csv",
                    "target_table": "iceberg_data.default.table1",
                    "create_time": "2024-01-01T00:00:00Z",
                },
                {
                    "job_id": "job-456",
                    "status": "completed",
                    "source_data_files": "s3://bucket/data/input2.parquet",
                    "target_table": "iceberg_data.default.table2",
                    "create_time": "2024-01-01T01:00:00Z",
                    "end_time": "2024-01-01T02:00:00Z",
                },
            ],
            "total_count": 2,
        }

        respx_mock.get("https://test.watsonx.com/api/v3/lhingestion/api/v1/ingestion/jobs").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await list_ingestion_jobs.fn(
            mock_context,
        )

        assert len(result["ingestion_jobs"]) == 2
        assert result["ingestion_jobs"][0]["job_id"] == "job-123"
        assert result["ingestion_jobs"][0]["status"] == "running"
        assert result["ingestion_jobs"][1]["job_id"] == "job-456"
        assert result["ingestion_jobs"][1]["status"] == "completed"

    @pytest.mark.asyncio
    async def test_list_jobs_with_pagination(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test listing ingestion jobs with pagination."""
        mock_response = {
            "ingestion_jobs": [
                {
                    "job_id": "job-123",
                    "status": "running",
                    "source_data_files": "s3://bucket/data/input.csv",
                    "target_table": "iceberg_data.default.table",
                    "create_time": "2024-01-01T00:00:00Z",
                },
            ],
            "total_count": 50,
            "page": 2,
        }

        respx_mock.get("https://test.watsonx.com/api/v3/lhingestion/api/v1/ingestion/jobs").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await list_ingestion_jobs.fn(
            mock_context,
            start=10,
            limit=10,
        )

        assert len(result["ingestion_jobs"]) == 1
        assert result["total_count"] == 50
        assert result["page"] == 2

    @pytest.mark.asyncio
    async def test_list_jobs_empty(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test listing ingestion jobs when none exist."""
        mock_response = {
            "ingestion_jobs": [],
            "total_count": 0,
        }

        respx_mock.get("https://test.watsonx.com/api/v3/lhingestion/api/v1/ingestion/jobs").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await list_ingestion_jobs.fn(
            mock_context,
        )

        assert len(result["ingestion_jobs"]) == 0
        assert result["total_count"] == 0


class TestGetIngestionJob:
    """Tests for get_ingestion_job tool."""

    @pytest.mark.asyncio
    async def test_get_running_job(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test getting status of a running ingestion job."""
        mock_response = {
            "job_id": "job-123",
            "status": "running",
            "source_data_files": "s3://bucket/data/input.csv",
            "target_table": "iceberg_data.default.my_table",
            "username": "test_user",
            "create_time": "2024-01-01T00:00:00Z",
            "start_time": "2024-01-01T00:01:00Z",
            "progress": 45.5,
            "rows_processed": 1000000,
        }

        respx_mock.get("https://test.watsonx.com/api/v3/lhingestion/api/v1/ingestion/jobs/job-123").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await get_ingestion_job.fn(
            mock_context,
            job_id="job-123",
        )

        assert result["job_id"] == "job-123"
        assert result["status"] == "running"
        assert result["progress"] == 45.5
        assert result["rows_processed"] == 1000000

    @pytest.mark.asyncio
    async def test_get_completed_job(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test getting status of a completed ingestion job."""
        mock_response = {
            "job_id": "job-456",
            "status": "completed",
            "source_data_files": "s3://bucket/data/input.parquet",
            "target_table": "iceberg_data.default.completed_table",
            "username": "test_user",
            "create_time": "2024-01-01T00:00:00Z",
            "start_time": "2024-01-01T00:01:00Z",
            "end_time": "2024-01-01T01:00:00Z",
            "rows_processed": 5000000,
            "bytes_processed": 1073741824,
        }

        respx_mock.get("https://test.watsonx.com/api/v3/lhingestion/api/v1/ingestion/jobs/job-456").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await get_ingestion_job.fn(
            mock_context,
            job_id="job-456",
        )

        assert result["job_id"] == "job-456"
        assert result["status"] == "completed"
        assert "end_time" in result
        assert result["rows_processed"] == 5000000

    @pytest.mark.asyncio
    async def test_get_failed_job(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test getting status of a failed ingestion job."""
        mock_response = {
            "job_id": "job-789",
            "status": "failed",
            "source_data_files": "s3://bucket/data/bad-input.csv",
            "target_table": "iceberg_data.default.failed_table",
            "username": "test_user",
            "create_time": "2024-01-01T00:00:00Z",
            "start_time": "2024-01-01T00:01:00Z",
            "end_time": "2024-01-01T00:05:00Z",
            "error_message": "Schema mismatch: expected INT but found STRING in column 'id'",
        }

        respx_mock.get("https://test.watsonx.com/api/v3/lhingestion/api/v1/ingestion/jobs/job-789").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await get_ingestion_job.fn(
            mock_context,
            job_id="job-789",
        )

        assert result["job_id"] == "job-789"
        assert result["status"] == "failed"
        assert "error_message" in result
        assert "Schema mismatch" in result["error_message"]

    @pytest.mark.asyncio
    async def test_get_queued_job(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test getting status of a queued ingestion job."""
        mock_response = {
            "job_id": "job-abc",
            "status": "queued",
            "source_data_files": "s3://bucket/data/input.json",
            "target_table": "iceberg_data.default.queued_table",
            "username": "test_user",
            "create_time": "2024-01-01T00:00:00Z",
        }

        respx_mock.get("https://test.watsonx.com/api/v3/lhingestion/api/v1/ingestion/jobs/job-abc").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await get_ingestion_job.fn(
            mock_context,
            job_id="job-abc",
        )

        assert result["job_id"] == "job-abc"
        assert result["status"] == "queued"
        assert "start_time" not in result


class TestDeleteIngestionJob:
    """Tests for delete_ingestion_job tool."""

    @pytest.mark.asyncio
    async def test_delete_job_success(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test successfully deleting an ingestion job."""
        mock_response = {
            "message": "Ingestion job deleted successfully",
        }

        respx_mock.delete("https://test.watsonx.com/api/v3/ingestion_jobs/job-123").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await delete_ingestion_job.fn(
            mock_context,
            job_id="job-123",
        )

        assert "message" in result

    @pytest.mark.asyncio
    async def test_delete_running_job(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test deleting a running ingestion job (should cancel it first)."""
        mock_response = {
            "message": "Ingestion job cancelled and deleted",
        }

        respx_mock.delete("https://test.watsonx.com/api/v3/ingestion_jobs/job-running").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await delete_ingestion_job.fn(
            mock_context,
            job_id="job-running",
        )

        assert "message" in result

    @pytest.mark.asyncio
    async def test_delete_nonexistent_job(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test deleting a non-existent ingestion job."""
        respx_mock.delete("https://test.watsonx.com/api/v3/ingestion_jobs/job-nonexistent").mock(
            return_value=httpx.Response(404, json={"error": "Ingestion job not found"})
        )

        with pytest.raises(Exception):
            await delete_ingestion_job.fn(
                mock_context,
                job_id="job-nonexistent",
            )

    @pytest.mark.asyncio
    async def test_delete_completed_job(
        self,
        mock_context,
        watsonx_client,
        respx_mock,
    ):
        """Test deleting a completed ingestion job."""
        mock_response = {
            "message": "Ingestion job deleted successfully",
        }

        respx_mock.delete("https://test.watsonx.com/api/v3/ingestion_jobs/job-completed").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await delete_ingestion_job.fn(
            mock_context,
            job_id="job-completed",
        )

        assert "message" in result