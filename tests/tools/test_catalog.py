"""
Tests for catalog tools.

This file has been modified with the assistance of IBM Bob AI tool
"""

import httpx
import pytest

from lakehouse_mcp.tools.catalog.describe_table import describe_table
from lakehouse_mcp.tools.catalog.list_schemas import list_schemas
from lakehouse_mcp.tools.catalog.list_tables import list_tables


class TestListSchemas:
    """Tests for list_schemas tool."""

    @pytest.mark.asyncio
    async def test_list_schemas_success(self, mock_context, watsonx_client, respx_mock):
        """Test listing schemas from a catalog."""
        mock_response = {"schemas": ["default", "analytics", "reporting"]}

        respx_mock.get("https://test.watsonx.com/api/v2/catalogs/iceberg_data/schemas?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await list_schemas.fn(mock_context, catalog_name="iceberg_data", engine_id="presto-01")

        assert result["total_count"] == 3
        assert len(result["schemas"]) == 3
        assert result["catalog_name"] == "iceberg_data"
        assert result["engine_id"] == "presto-01"

        # Verify schema details
        schema_names = [s["schema_name"] for s in result["schemas"]]
        assert "default" in schema_names
        assert "analytics" in schema_names
        assert "reporting" in schema_names

        # All schemas should have catalog_name set
        assert all(s["catalog_name"] == "iceberg_data" for s in result["schemas"])

    @pytest.mark.asyncio
    async def test_list_schemas_tpch(self, mock_context, watsonx_client, respx_mock):
        """Test listing schemas from tpch catalog."""
        mock_response = {"schemas": ["sf1", "sf10", "sf100"]}

        respx_mock.get("https://test.watsonx.com/api/v2/catalogs/tpch/schemas?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await list_schemas.fn(mock_context, catalog_name="tpch", engine_id="presto-01")

        assert result["total_count"] == 3
        assert result["catalog_name"] == "tpch"
        assert all(s["catalog_name"] == "tpch" for s in result["schemas"])

    @pytest.mark.asyncio
    async def test_list_schemas_empty_response(self, mock_context, watsonx_client, respx_mock):
        """Test listing schemas with empty response."""
        respx_mock.get("https://test.watsonx.com/api/v2/catalogs/empty_catalog/schemas?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json={"schemas": []})
        )

        result = await list_schemas.fn(mock_context, catalog_name="empty_catalog", engine_id="presto-01")

        assert result["total_count"] == 0
        assert result["schemas"] == []


class TestListTables:
    """Tests for list_tables tool."""

    @pytest.mark.asyncio
    async def test_list_tables_success(self, mock_context, watsonx_client, respx_mock, mock_tables_response):
        """Test successful table listing."""
        respx_mock.get("https://test.watsonx.com/api/v2/catalogs/iceberg_data/schemas/sales_db/tables?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_tables_response)
        )

        result = await list_tables.fn(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        assert result["total_count"] == 3
        assert len(result["tables"]) == 3
        assert result["catalog_name"] == "iceberg_data"
        assert result["schema_name"] == "sales_db"
        assert result["engine_id"] == "presto-01"

        # Verify table details (API returns string array, so metadata is minimal)
        table_names = [t["name"] for t in result["tables"]]
        assert "customers" in table_names
        assert "orders" in table_names
        assert "customer_view" in table_names

    @pytest.mark.asyncio
    async def test_list_tables_with_engine_id(self, mock_context, watsonx_client, respx_mock):
        """Test listing tables with specific engine."""
        mock_response = {"tables": []}

        respx_mock.get("https://test.watsonx.com/api/v2/catalogs/iceberg_data/schemas/sales_db/tables?engine_id=presto-02").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await list_tables.fn(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-02",
        )

        assert result["engine_id"] == "presto-02"

    @pytest.mark.asyncio
    async def test_list_tables_empty_schema(self, mock_context, watsonx_client, respx_mock):
        """Test listing tables from empty schema."""
        respx_mock.get("https://test.watsonx.com/api/v2/catalogs/iceberg_data/schemas/empty_schema/tables?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json={"tables": []})
        )

        result = await list_tables.fn(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="empty_schema",
            engine_id="presto-01",
        )

        assert result["total_count"] == 0
        assert result["tables"] == []

    @pytest.mark.asyncio
    async def test_list_tables_with_metadata(self, mock_context, watsonx_client, respx_mock):
        """Test that table metadata is properly extracted when API returns dict objects."""
        # Test backward compatibility: if API returns dict objects with metadata
        mock_response = {
            "tables": [
                {
                    "table_name": "products",
                    "table_type": "TABLE",
                    "row_count": 10000,
                    "size_bytes": 500000,
                    "created_on": "2024-01-15T10:00:00Z",
                    "columns": [{"name": "id"}, {"name": "name"}],
                }
            ]
        }

        respx_mock.get("https://test.watsonx.com/api/v2/catalogs/iceberg_data/schemas/sales_db/tables?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await list_tables.fn(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        table = result["tables"][0]
        assert table["name"] == "products"
        assert table["type"] == "TABLE"
        assert table["row_count"] == 10000
        assert table["size_bytes"] == 500000
        assert table["column_count"] == 2

    @pytest.mark.asyncio
    async def test_list_tables_alternative_field_names(self, mock_context, watsonx_client, respx_mock):
        """Test listing tables with alternative field names."""
        mock_response = {
            "tables": [
                {
                    "name": "test_table",  # "name" instead of "table_name"
                    "type": "VIEW",  # "type" instead of "table_type"
                    "size": 1000,  # "size" instead of "size_bytes"
                    "created_at": "2024-01-01T00:00:00Z",  # "created_at" instead of "created_on"
                }
            ]
        }

        respx_mock.get("https://test.watsonx.com/api/v2/catalogs/test/schemas/test/tables?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await list_tables.fn(
            mock_context,
            catalog_name="test",
            schema_name="test",
            engine_id="presto-01",
        )

        table = result["tables"][0]
        assert table["name"] == "test_table"
        assert table["type"] == "VIEW"
        assert table["size_bytes"] == 1000
        assert table["created_on"] == "2024-01-01T00:00:00Z"

    @pytest.mark.asyncio
    async def test_list_tables_api_error(self, mock_context, watsonx_client, respx_mock):
        """Test listing tables with API error."""
        respx_mock.get("https://test.watsonx.com/api/v2/catalogs/invalid/schemas/invalid/tables?engine_id=presto-01").mock(
            return_value=httpx.Response(404, json={"error": "Schema not found"})
        )

        with pytest.raises(httpx.HTTPStatusError):
            await list_tables.fn(
                mock_context,
                catalog_name="invalid",
                schema_name="invalid",
                engine_id="presto-01",
            )


class TestDescribeTable:
    """Tests for describe_table tool."""

    @pytest.mark.asyncio
    async def test_describe_table_success(self, mock_context, watsonx_client, respx_mock, mock_describe_table_response):
        """Test successful table description."""
        respx_mock.get("https://test.watsonx.com/api/v2/catalogs/iceberg_data/schemas/sales_db/tables/customers?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_describe_table_response)
        )

        result = await describe_table.fn(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            table_name="customers",
            engine_id="presto-01",
        )

        assert result["table_name"] == "customers"
        assert result["catalog_name"] == "iceberg_data"
        assert result["schema_name"] == "sales_db"
        assert result["column_count"] == 4
        assert len(result["columns"]) == 4
        assert result["partitions"] == ["created_at"]
        assert result["table_type"] == "MANAGED_TABLE"
        assert result["engine_id"] == "presto-01"

    @pytest.mark.asyncio
    async def test_describe_table_columns(self, mock_context, watsonx_client, respx_mock, mock_describe_table_response):
        """Test that column details are properly extracted."""
        respx_mock.get("https://test.watsonx.com/api/v2/catalogs/iceberg_data/schemas/sales_db/tables/customers?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_describe_table_response)
        )

        result = await describe_table.fn(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            table_name="customers",
            engine_id="presto-01",
        )

        columns = result["columns"]
        assert len(columns) == 4

        # Check first column
        id_col = columns[0]
        assert id_col["column_name"] == "id"
        assert id_col["data_type"] == "bigint"
        assert id_col["nullable"] is False
        assert id_col["comment"] == "Primary key"

        # Check nullable column
        email_col = columns[2]
        assert email_col["column_name"] == "email"
        assert email_col["nullable"] is True

    @pytest.mark.asyncio
    async def test_describe_table_with_engine_id(self, mock_context, watsonx_client, respx_mock):
        """Test describing table with engine_id."""
        mock_response = {
            "table_type": "TABLE",
            "columns": [],
            "primary_keys": [],
            "partitions": [],
            "properties": {},
        }

        respx_mock.get("https://test.watsonx.com/api/v2/catalogs/iceberg_data/schemas/sales_db/tables/test?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await describe_table.fn(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            table_name="test",
            engine_id="presto-01",
        )

        assert result["engine_id"] == "presto-01"

    @pytest.mark.asyncio
    async def test_describe_table_with_statistics(self, mock_context, watsonx_client, respx_mock):
        """Test that table statistics are properly extracted."""
        mock_response = {
            "table_type": "TABLE",
            "columns": [],
            "primary_keys": [],
            "partitions": [],
            "properties": {},
            "row_count": 50000,
            "size_bytes": 2500000,
            "created_on": "2024-01-15T10:00:00Z",
            "last_modified": "2024-01-20T15:30:00Z",
        }

        respx_mock.get("https://test.watsonx.com/api/v2/catalogs/iceberg_data/schemas/sales_db/tables/products?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await describe_table.fn(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            table_name="products",
            engine_id="presto-01",
        )

        stats = result["statistics"]
        assert stats["row_count"] == 50000
        assert stats["size_bytes"] == 2500000
        assert stats["created_on"] == "2024-01-15T10:00:00Z"
        assert stats["last_modified"] == "2024-01-20T15:30:00Z"

    @pytest.mark.asyncio
    async def test_describe_table_alternative_field_names(self, mock_context, watsonx_client, respx_mock):
        """Test describing table with alternative field names."""
        mock_response = {
            "table": {  # Nested under "table"
                "table_type": "VIEW",
                "columns": [
                    {
                        "name": "col1",  # "name" instead of "column_name"
                        "type": "string",  # "type" instead of "data_type"
                        "is_nullable": False,  # "is_nullable" instead of "nullable"
                        "description": "Test column",  # "description" instead of "comment"
                        "position": 1,  # "position" instead of "ordinal_position"
                    }
                ],
                "size": 1000,  # "size" instead of "size_bytes"
                "created_at": "2024-01-01T00:00:00Z",  # "created_at" instead of "created_on"
                "updated_at": "2024-01-02T00:00:00Z",  # "updated_at" instead of "last_modified"
            }
        }

        respx_mock.get("https://test.watsonx.com/api/v2/catalogs/test/schemas/test/tables/test?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await describe_table.fn(
            mock_context,
            catalog_name="test",
            schema_name="test",
            table_name="test",
            engine_id="presto-01",
        )

        # Check column with alternative names
        col = result["columns"][0]
        assert col["column_name"] == "col1"
        assert col["data_type"] == "string"
        assert col["nullable"] is False
        assert col["comment"] == "Test column"
        assert col["ordinal_position"] == 1

        # Check statistics with alternative names
        assert result["statistics"]["size_bytes"] == 1000
        assert result["statistics"]["created_on"] == "2024-01-01T00:00:00Z"
        assert result["statistics"]["last_modified"] == "2024-01-02T00:00:00Z"

    @pytest.mark.asyncio
    async def test_describe_table_not_found(self, mock_context, watsonx_client, respx_mock):
        """Test describing non-existent table."""
        respx_mock.get(
            "https://test.watsonx.com/api/v2/catalogs/iceberg_data/schemas/sales_db/tables/nonexistent?engine_id=presto-01"
        ).mock(return_value=httpx.Response(404, json={"error": "Table not found"}))

        with pytest.raises(httpx.HTTPStatusError):
            await describe_table.fn(
                mock_context,
                catalog_name="iceberg_data",
                schema_name="sales_db",
                table_name="nonexistent",
                engine_id="presto-01",
            )

    @pytest.mark.asyncio
    async def test_describe_table_no_partitions(self, mock_context, watsonx_client, respx_mock):
        """Test describing table without partitions."""
        mock_response = {
            "table_type": "TABLE",
            "columns": [{"column_name": "id", "data_type": "int", "nullable": False}],
            "primary_keys": ["id"],
            "partitions": [],
            "properties": {},
        }

        respx_mock.get("https://test.watsonx.com/api/v2/catalogs/test/schemas/test/tables/test?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await describe_table.fn(
            mock_context,
            catalog_name="test",
            schema_name="test",
            table_name="test",
            engine_id="presto-01",
        )

        assert result["partitions"] == []
        assert result["primary_keys"] == ["id"]
