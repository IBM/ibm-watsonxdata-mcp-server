"""
Create ingestion job tool.

This tool creates a data ingestion job to load data into watsonx.data.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

import httpx
from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def create_ingestion_job(
    ctx: Context,
    job_id: str,
    catalog: str,
    schema: str,
    table: str,
    file_paths: str,
    file_type: str = "csv",
    bucket_name: str | None = None,
    bucket_type: str = "ibm_cos",
    write_mode: str = "append",
    engine_id: str | None = None,
    field_delimiter: str = ",",
    line_delimiter: str = "\n",
    escape_character: str = "\\",
    header: bool = True,
    encoding: str = "UTF-8",
    driver_memory: str = "2G",
    driver_cores: int = 1,
    executor_memory: str = "2G",
    executor_cores: int = 1,
    num_executors: int = 1,
) -> dict[str, Any]:
    """Create a data ingestion job to load data into watsonx.data.

    Args:
        job_id: Unique job identifier (e.g., "ingestion-1234567890")
        catalog: Target catalog name
        schema: Target schema name
        table: Target table name
        file_paths: Source file path (e.g., "s3://bucket-name/file.csv")
        file_type: Source file type - "csv", "parquet", "json" (default: "csv")
        bucket_name: S3 bucket name (extracted from file_paths if not provided)
        bucket_type: Bucket type - "ibm_cos", "aws_s3" (default: "ibm_cos")
        write_mode: Write mode - "append", "overwrite" (default: "append")
        engine_id: Spark engine ID to use for ingestion
        field_delimiter: CSV field delimiter (default: ",")
        line_delimiter: CSV line delimiter (default: "\n")
        escape_character: CSV escape character (default: "\\")
        header: Whether CSV has header row (default: true)
        encoding: File encoding (default: "UTF-8")
        driver_memory: Spark driver memory (default: "2G")
        driver_cores: Spark driver cores (default: 1)
        executor_memory: Spark executor memory (default: "2G")
        executor_cores: Spark executor cores (default: 1)
        num_executors: Number of Spark executors (default: 1)

    Returns:
        Dict with job_id, status, and creation details
    """
    watsonx_client = ctx.fastmcp.dependencies["watsonx_client"]

    # Extract bucket_name from file_paths if not provided
    if bucket_name is None and file_paths.startswith("s3://"):
        bucket_name = file_paths.split("/")[2]

    # Build v3 API request body with nested structure
    body: dict[str, Any] = {
        "job_id": job_id,
        "target": {
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "write_mode": write_mode,
        },
        "source": {
            "file_paths": file_paths,
            "file_type": file_type,
            "bucket_details": {
                "bucket_name": bucket_name,
                "bucket_type": bucket_type,
            },
        },
        "execute_config": {
            "driver_memory": driver_memory,
            "driver_cores": driver_cores,
            "executor_memory": executor_memory,
            "executor_cores": executor_cores,
            "num_executors": num_executors,
        },
    }

    # Add file format properties for CSV files
    if file_type == "csv":
        body["source"]["file_format_properties"] = {
            "field_delimiter": field_delimiter,
            "line_delimiter": line_delimiter,
            "escape_character": escape_character,
            "header": header,
            "encoding": encoding,
        }

    # Add engine_id if provided
    if engine_id is not None:
        body["engine_id"] = engine_id

    logger.info(
        "creating_ingestion_job",
        job_id=job_id,
        file_paths=file_paths,
        file_type=file_type,
        catalog=catalog,
        schema=schema,
        table=table,
    )

    try:
        path = "/v3/lhingestion/api/v1/ingestion/jobs"
        response = await watsonx_client.post(path, body)

        logger.info(
            "ingestion_job_created",
            job_id=response.get("job_id"),
            status=response.get("status"),
        )

        return response

    except Exception as e:
        logger.error(
            "ingestion_job_creation_failed",
            job_id=job_id,
            catalog=catalog,
            schema=schema,
            table=table,
            error=str(e),
        )
        raise