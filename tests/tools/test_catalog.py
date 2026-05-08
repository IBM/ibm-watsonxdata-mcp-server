"""
Tests for catalog tools.

This file has been modified with the assistance of IBM Bob AI tool
"""

import httpx
import pytest

from lakehouse_mcp.tools.catalog.describe_table import describe_table
from lakehouse_mcp.tools.catalog.list_schemas import list_schemas
from lakehouse_mcp.tools.catalog.create_schema import create_schema
from lakehouse_mcp.tools.catalog.list_tables import list_tables


class TestListSchemas:
    """Tests for list_schemas tool."""

    @pytest.mark.asyncio
    async def test_list_schemas_success(self, mock_context, watsonx_client, respx_mock):
        """Test listing schemas from a catalog."""
        mock_response = {"schemas": ["default", "analytics", "reporting"]}

        respx_mock.get("https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await list_schemas(mock_context, catalog_name="iceberg_data", engine_id="presto-01")

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

        respx_mock.get("https://test.watsonx.com/api/v3/catalogs/tpch/schemas?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await list_schemas(mock_context, catalog_name="tpch", engine_id="presto-01")

        assert result["total_count"] == 3
        assert result["catalog_name"] == "tpch"
        assert all(s["catalog_name"] == "tpch" for s in result["schemas"])

    @pytest.mark.asyncio
    async def test_list_schemas_empty_response(self, mock_context, watsonx_client, respx_mock):
        """Test listing schemas with empty response."""
        respx_mock.get("https://test.watsonx.com/api/v3/catalogs/empty_catalog/schemas?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json={"schemas": []})
        )

        result = await list_schemas(mock_context, catalog_name="empty_catalog", engine_id="presto-01")

        assert result["total_count"] == 0
        assert result["schemas"] == []


class TestListTables:
    """Tests for list_tables tool."""

    @pytest.mark.asyncio
    async def test_list_tables_success(self, mock_context, watsonx_client, respx_mock, mock_tables_response):
        """Test successful table listing."""
        respx_mock.get("https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas/sales_db/tables?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_tables_response)
        )

        result = await list_tables(
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

        # Verify table names (API returns string array)
        assert "customers" in result["tables"]
        assert "orders" in result["tables"]
        assert "customer_view" in result["tables"]

    @pytest.mark.asyncio
    async def test_list_tables_with_engine_id(self, mock_context, watsonx_client, respx_mock):
        """Test listing tables with specific engine."""
        mock_response = {"tables": []}

        respx_mock.get("https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas/sales_db/tables?engine_id=presto-02").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await list_tables(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-02",
        )

        assert result["engine_id"] == "presto-02"

    @pytest.mark.asyncio
    async def test_list_tables_empty_schema(self, mock_context, watsonx_client, respx_mock):
        """Test listing tables from empty schema."""
        respx_mock.get("https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas/empty_schema/tables?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json={"tables": []})
        )

        result = await list_tables(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="empty_schema",
            engine_id="presto-01",
        )

        assert result["total_count"] == 0
        assert result["tables"] == []

    @pytest.mark.asyncio
    async def test_list_tables_api_error(self, mock_context, watsonx_client, respx_mock):
        """Test listing tables with API error."""
        respx_mock.get("https://test.watsonx.com/api/v3/catalogs/invalid/schemas/invalid/tables?engine_id=presto-01").mock(
            return_value=httpx.Response(404, json={"message": "Schema not found"})
        )

        result = await list_tables(
            mock_context,
            catalog_name="invalid",
            schema_name="invalid",
            engine_id="presto-01",
        )
        
        assert result["error"] is True
        assert "Schema not found" in result["error_message"]
        assert result["status_code"] == 404


class TestDescribeTable:
    """Tests for describe_table tool."""

    @pytest.mark.asyncio
    async def test_describe_table_success(self, mock_context, watsonx_client, respx_mock, mock_describe_table_response):
        """Test successful table description."""
        respx_mock.get("https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas/sales_db/tables/customers?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_describe_table_response)
        )

        result = await describe_table(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            table_name="customers",
            engine_id="presto-01",
        )

        assert result["name"] == "customers"
        assert result["catalog_name"] == "iceberg_data"
        assert result["schema_name"] == "sales_db"
        assert result["column_count"] == 4
        assert len(result["columns"]) == 4
        assert result["engine_id"] == "presto-01"

    @pytest.mark.asyncio
    async def test_describe_table_columns(self, mock_context, watsonx_client, respx_mock, mock_describe_table_response):
        """Test that column details are properly extracted."""
        respx_mock.get("https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas/sales_db/tables/customers?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_describe_table_response)
        )

        result = await describe_table(
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
        assert id_col["name"] == "id"
        assert id_col["type"] == "bigint"
        assert id_col.get("comment") == "Primary key"

        # Check nullable column
        email_col = columns[2]
        assert email_col["name"] == "email"

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

        respx_mock.get("https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas/sales_db/tables/test?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await describe_table(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            table_name="test",
            engine_id="presto-01",
        )

        assert result["engine_id"] == "presto-01"

    @pytest.mark.asyncio
    async def test_describe_table_not_found(self, mock_context, watsonx_client, respx_mock):
        """Test describing non-existent table."""
        respx_mock.get(
            "https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas/sales_db/tables/nonexistent?engine_id=presto-01"
        ).mock(return_value=httpx.Response(404, json={"message": "Table not found"}))

        result = await describe_table(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            table_name="nonexistent",
            engine_id="presto-01",
        )
        
        assert result["error"] is True
        assert "Table not found" in result["error_message"]
        assert result["status_code"] == 404


class TestCreateSchema:
    """Tests for create_schema tool."""

    @pytest.mark.asyncio
    async def test_create_schema_basic(self, mock_context, watsonx_client, respx_mock):
        """Test creating a basic schema."""
        mock_response = {
            "name": "new_schema",
            "catalog_name": "iceberg_data",
            "custom_path": "new_schema",
        }

        respx_mock.post("https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await create_schema(
            mock_context,
            catalog_id="iceberg_data",
            schema_name="new_schema",
            engine_id="presto-01",
            custom_path="new_schema",
        )

        assert result["name"] == "new_schema"
        assert result["catalog_name"] == "iceberg_data"

    @pytest.mark.asyncio
    async def test_create_schema_with_custom_path(self, mock_context, watsonx_client, respx_mock):
        """Test creating a schema with custom path."""
        mock_response = {
            "name": "custom_schema",
            "catalog_name": "iceberg_data",
            "custom_path": "/data/schemas/custom",
        }

        respx_mock.post("https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await create_schema(
            mock_context,
            catalog_id="iceberg_data",
            schema_name="custom_schema",
            engine_id="presto-01",
            custom_path="/data/schemas/custom",
        )

        assert result["name"] == "custom_schema"
        assert result["custom_path"] == "/data/schemas/custom"

    @pytest.mark.asyncio
    async def test_create_schema_with_storage(self, mock_context, watsonx_client, respx_mock):
        """Test creating a schema with storage name."""
        mock_response = {
            "name": "storage_schema",
            "catalog_name": "iceberg_data",
            "custom_path": "storage_schema",
            "storage_name": "my-bucket",
        }

        respx_mock.post("https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await create_schema(
            mock_context,
            catalog_id="iceberg_data",
            schema_name="storage_schema",
            engine_id="presto-01",
            custom_path="storage_schema",
            storage_name="my-bucket",
        )

        assert result["name"] == "storage_schema"
        assert result["storage_name"] == "my-bucket"

    @pytest.mark.asyncio
    async def test_create_schema_already_exists(self, mock_context, watsonx_client, respx_mock):
        """Test creating a schema that already exists."""
        respx_mock.post("https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas?engine_id=presto-01").mock(
            return_value=httpx.Response(409, json={"message": "Schema already exists"})
        )

        result = await create_schema(
            mock_context,
            catalog_id="iceberg_data",
            schema_name="existing_schema",
            engine_id="presto-01",
            custom_path="existing_schema",
        )
        
        assert result["error"] is True
        assert "Schema already exists" in result["error_message"]
        assert result["status_code"] == 409

    @pytest.mark.asyncio
    async def test_create_schema_invalid_catalog(self, mock_context, watsonx_client, respx_mock):
        """Test creating a schema in non-existent catalog."""
        respx_mock.post("https://test.watsonx.com/api/v3/catalogs/invalid_catalog/schemas?engine_id=presto-01").mock(
            return_value=httpx.Response(404, json={"message": "Catalog not found"})
        )

        result = await create_schema(
            mock_context,
            catalog_id="invalid_catalog",
            schema_name="new_schema",
            engine_id="presto-01",
            custom_path="new_schema",
        )
        
        assert result["error"] is True
        assert "Catalog not found" in result["error_message"]
        assert result["status_code"] == 404
        


class TestRenameTable:
    """Tests for rename_table tool."""

    @pytest.mark.asyncio
    async def test_rename_table_success(self, mock_context, watsonx_client, respx_mock):
        """Test successfully renaming a table."""
        from lakehouse_mcp.tools.catalog.rename_table import rename_table
        
        mock_response = {
            "name": "customers_new",
            "catalog_name": "iceberg_data",
            "schema_name": "sales_db",
        }

        respx_mock.patch(
            "https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas/sales_db/tables/customers?engine_id=presto-01"
        ).mock(return_value=httpx.Response(200, json=mock_response))

        result = await rename_table(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            table_name="customers",
            new_table_name="customers_new",
            engine_id="presto-01",
        )

        assert result["name"] == "customers_new"
        assert result["catalog_name"] == "iceberg_data"
        assert result["schema_name"] == "sales_db"

    @pytest.mark.asyncio
    async def test_rename_table_not_found(self, mock_context, watsonx_client, respx_mock):
        """Test renaming a non-existent table."""
        from lakehouse_mcp.tools.catalog.rename_table import rename_table
        
        respx_mock.patch(
            "https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas/sales_db/tables/nonexistent?engine_id=presto-01"
        ).mock(return_value=httpx.Response(404, json={"message": "Table not found"}))

        result = await rename_table(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            table_name="nonexistent",
            new_table_name="new_name",
            engine_id="presto-01",
        )
        
        assert result["error"] is True
        assert "Table not found" in result["error_message"]
        assert result["status_code"] == 404

    @pytest.mark.asyncio
    async def test_rename_table_name_conflict(self, mock_context, watsonx_client, respx_mock):
        """Test renaming a table to an existing name."""
        from lakehouse_mcp.tools.catalog.rename_table import rename_table
        
        respx_mock.patch(
            "https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas/sales_db/tables/customers?engine_id=presto-01"
        ).mock(return_value=httpx.Response(409, json={"message": "Table name already exists"}))

        result = await rename_table(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            table_name="customers",
            new_table_name="orders",
            engine_id="presto-01",
        )
        
        assert result["error"] is True
        assert "Table name already exists" in result["error_message"]
        assert result["status_code"] == 409


class TestAddColumns:
    """Tests for add_columns tool."""

    @pytest.mark.asyncio
    async def test_add_columns_single(self, mock_context, watsonx_client, respx_mock):
        """Test adding a single column to a table."""
        from lakehouse_mcp.tools.catalog.add_columns import add_columns
        
        mock_response = {
            "columns": [
                {
                    "name": "new_column",
                    "type": "string",
                    "comment": "A new column",
                }
            ],
            "total_count": 1,
        }

        respx_mock.post(
            "https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas/sales_db/tables/customers/columns?engine_id=presto-01"
        ).mock(return_value=httpx.Response(200, json=mock_response))

        columns = [
            {
                "name": "new_column",
                "type": "string",
                "comment": "A new column",
            }
        ]

        result = await add_columns(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            table_name="customers",
            columns=columns,
            engine_id="presto-01",
        )

        assert result["total_count"] == 1
        assert len(result["columns"]) == 1
        assert result["columns"][0]["name"] == "new_column"

    @pytest.mark.asyncio
    async def test_add_columns_multiple(self, mock_context, watsonx_client, respx_mock):
        """Test adding multiple columns to a table."""
        from lakehouse_mcp.tools.catalog.add_columns import add_columns
        
        mock_response = {
            "columns": [
                {"name": "col1", "type": "int"},
                {"name": "col2", "type": "string"},
                {"name": "col3", "type": "boolean"},
            ],
            "total_count": 3,
        }

        respx_mock.post(
            "https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas/sales_db/tables/customers/columns?engine_id=presto-01"
        ).mock(return_value=httpx.Response(200, json=mock_response))

        columns = [
            {"name": "col1", "type": "int"},
            {"name": "col2", "type": "string"},
            {"name": "col3", "type": "boolean"},
        ]

        result = await add_columns(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            table_name="customers",
            columns=columns,
            engine_id="presto-01",
        )

        assert result["total_count"] == 3
        assert len(result["columns"]) == 3

    @pytest.mark.asyncio
    async def test_add_columns_with_precision_scale(self, mock_context, watsonx_client, respx_mock):
        """Test adding columns with precision and scale."""
        from lakehouse_mcp.tools.catalog.add_columns import add_columns
        
        mock_response = {
            "columns": [
                {
                    "name": "price",
                    "type": "decimal",
                    "precision": 10,
                    "scale": 2,
                }
            ],
            "total_count": 1,
        }

        respx_mock.post(
            "https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas/sales_db/tables/products/columns?engine_id=presto-01"
        ).mock(return_value=httpx.Response(200, json=mock_response))

        columns = [
            {
                "name": "price",
                "type": "decimal",
                "precision": 10,
                "scale": 2,
            }
        ]

        result = await add_columns(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            table_name="products",
            columns=columns,
            engine_id="presto-01",
        )

        assert result["columns"][0]["precision"] == 10
        assert result["columns"][0]["scale"] == 2

    @pytest.mark.asyncio
    async def test_add_columns_table_not_found(self, mock_context, watsonx_client, respx_mock):
        """Test adding columns to non-existent table."""
        from lakehouse_mcp.tools.catalog.add_columns import add_columns
        
        respx_mock.post(
            "https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas/sales_db/tables/nonexistent/columns?engine_id=presto-01"
        ).mock(return_value=httpx.Response(404, json={"message": "Table not found"}))

        columns = [{"name": "new_col", "type": "string"}]

        result = await add_columns(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            table_name="nonexistent",
            columns=columns,
            engine_id="presto-01",
        )
        
        assert result["error"] is True
        assert "Table not found" in result["error_message"]
        assert result["status_code"] == 404


class TestRenameColumn:
    """Tests for rename_column tool."""

    @pytest.mark.asyncio
    async def test_rename_column_success(self, mock_context, watsonx_client, respx_mock):
        """Test successfully renaming a column."""
        from lakehouse_mcp.tools.catalog.rename_column import rename_column
        
        mock_response = {
            "name": "customer_email",
            "type": "string",
            "nullable": True,
        }

        respx_mock.patch(
            "https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas/sales_db/tables/customers/columns/email?engine_id=presto-01"
        ).mock(return_value=httpx.Response(200, json=mock_response))

        result = await rename_column(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            table_name="customers",
            column_name="email",
            new_column_name="customer_email",
            engine_id="presto-01",
        )

        assert result["name"] == "customer_email"

    @pytest.mark.asyncio
    async def test_rename_column_not_found(self, mock_context, watsonx_client, respx_mock):
        """Test renaming a non-existent column."""
        from lakehouse_mcp.tools.catalog.rename_column import rename_column
        
        respx_mock.patch(
            "https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas/sales_db/tables/customers/columns/nonexistent?engine_id=presto-01"
        ).mock(return_value=httpx.Response(404, json={"message": "Column not found"}))

        result = await rename_column(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            table_name="customers",
            column_name="nonexistent",
            new_column_name="new_name",
            engine_id="presto-01",
        )
        
        assert result["error"] is True
        assert "Column not found" in result["error_message"]
        assert result["status_code"] == 404

    @pytest.mark.asyncio
    async def test_rename_column_name_conflict(self, mock_context, watsonx_client, respx_mock):
        """Test renaming a column to an existing name."""
        from lakehouse_mcp.tools.catalog.rename_column import rename_column
        
        respx_mock.patch(
            "https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas/sales_db/tables/customers/columns/email?engine_id=presto-01"
        ).mock(return_value=httpx.Response(409, json={"message": "Column name already exists"}))

        result = await rename_column(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            table_name="customers",
            column_name="email",
            new_column_name="id",
            engine_id="presto-01",
        )
        
        assert result["error"] is True
        assert "Column name already exists" in result["error_message"]
        assert result["status_code"] == 409

    @pytest.mark.asyncio
    async def test_rename_column_table_not_found(self, mock_context, watsonx_client, respx_mock):
        """Test renaming a column in non-existent table."""
        from lakehouse_mcp.tools.catalog.rename_column import rename_column
        
        respx_mock.patch(
            "https://test.watsonx.com/api/v3/catalogs/iceberg_data/schemas/sales_db/tables/nonexistent/columns/email?engine_id=presto-01"
        ).mock(return_value=httpx.Response(404, json={"message": "Table not found"}))

        result = await rename_column(
            mock_context,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            table_name="nonexistent",
            column_name="email",
            new_column_name="new_email",
            engine_id="presto-01",
        )
        
        assert result["error"] is True
        assert "Table not found" in result["error_message"]
        assert result["status_code"] == 404
