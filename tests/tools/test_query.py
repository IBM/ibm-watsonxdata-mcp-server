"""
Tests for query tools.

This file has been modified with the assistance of IBM Bob AI tool
"""

import httpx
import pytest

from lakehouse_mcp.tools.query.execute_select import execute_select


class TestExecuteSelect:
    """Tests for execute_select tool."""

    @pytest.mark.asyncio
    async def test_execute_select_success(self, mock_context, watsonx_client, respx_mock):
        """Test successful query execution."""
        mock_response = {
            "data": {
                "id": "query-123",
                "nextUri": "",
                "stats": {"state": "FINISHED"},
                "columns": [
                    {"name": "id", "type": "bigint"},
                    {"name": "name", "type": "varchar"},
                    {"name": "total", "type": "decimal"},
                ],
                "data": [[1, "Customer A", 1500.50], [2, "Customer B", 2300.75]],
            }
        }

        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await execute_select.fn(
            mock_context,
            sql="SELECT id, name, total FROM customers",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        assert len(result["columns"]) == 3
        assert result["row_count"] == 2
        assert len(result["rows"]) == 2
        assert result["catalog_name"] == "iceberg_data"
        assert result["schema_name"] == "sales_db"
        assert result["query_id"] == "query-123"

    @pytest.mark.asyncio
    async def test_execute_select_with_limit_param(self, mock_context, watsonx_client, respx_mock):
        """Test query execution with limit parameter."""
        mock_response = {
            "data": {
                "id": "query-456",
                "nextUri": "",
                "stats": {"state": "FINISHED"},
                "columns": [{"name": "id", "type": "bigint"}],
                "data": [[1]],
            }
        }

        route = respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        await execute_select.fn(
            mock_context,
            sql="SELECT * FROM customers",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
            limit=10,
        )

        # Verify limit was added to query
        request_body = route.calls[0].request.content
        assert b"LIMIT 10" in request_body

    @pytest.mark.asyncio
    async def test_execute_select_default_limit_500(self, mock_context, watsonx_client, respx_mock):
        """Test that default limit of 500 is applied when no limit specified."""
        mock_response = {
            "data": {
                "id": "query-789",
                "nextUri": "",
                "stats": {"state": "FINISHED"},
                "columns": [{"name": "id", "type": "bigint"}],
                "data": [[1]],
            }
        }

        route = respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        await execute_select.fn(
            mock_context,
            sql="SELECT * FROM customers",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        # Verify default limit of 500 was added
        request_body = route.calls[0].request.content
        assert b"LIMIT 500" in request_body

    @pytest.mark.asyncio
    async def test_execute_select_limit_in_query_not_duplicated(self, mock_context, watsonx_client, respx_mock):
        """Test that limit parameter doesn't override existing LIMIT in query."""
        mock_response = {
            "data": {
                "id": "query-101",
                "nextUri": "",
                "stats": {"state": "FINISHED"},
                "columns": [{"name": "id", "type": "bigint"}],
                "data": [[1]],
            }
        }

        route = respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        await execute_select.fn(
            mock_context,
            sql="SELECT * FROM customers LIMIT 5",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
            limit=10,  # Should not be added because LIMIT already exists
        )

        # Verify limit was not duplicated
        request_body = route.calls[0].request.content
        assert request_body.count(b"LIMIT") == 1
        assert b"LIMIT 5" in request_body
        assert b"LIMIT 10" not in request_body

    @pytest.mark.asyncio
    async def test_execute_select_invalid_query_not_select(self, mock_context):
        """Test that non-SELECT queries are rejected."""
        invalid_queries = [
            "INSERT INTO customers VALUES (1, 'test')",
            "UPDATE customers SET name = 'test'",
            "DELETE FROM customers WHERE id = 1",
            "DROP TABLE customers",
            "CREATE TABLE test (id INT)",
            "ALTER TABLE customers ADD COLUMN test VARCHAR",
        ]

        for query in invalid_queries:
            with pytest.raises(ValueError) as exc_info:
                await execute_select.fn(
                    mock_context,
                    sql=query,
                    catalog_name="iceberg_data",
                    schema_name="sales_db",
                    engine_id="presto-01",
                )

            assert "Only SELECT queries are allowed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_select_multiple_statements_rejected(self, mock_context):
        """Test that multiple statements are rejected."""
        query = "SELECT * FROM customers; DROP TABLE customers;"

        with pytest.raises(ValueError) as exc_info:
            await execute_select.fn(
                mock_context,
                sql=query,
                catalog_name="iceberg_data",
                schema_name="sales_db",
                engine_id="presto-01",
            )

        assert "Multiple statements not allowed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_select_trailing_semicolon_allowed(self, mock_context, watsonx_client, respx_mock):
        """Test that a single trailing semicolon is allowed."""
        mock_response = {
            "data": {
                "id": "query-202",
                "nextUri": "",
                "stats": {"state": "FINISHED"},
                "columns": [{"name": "id", "type": "bigint"}],
                "data": [[1]],
            }
        }

        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        # Should not raise an error
        result = await execute_select.fn(
            mock_context,
            sql="SELECT * FROM customers;",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        assert result["row_count"] == 1

    @pytest.mark.asyncio
    async def test_execute_select_case_insensitive_select(self, mock_context, watsonx_client, respx_mock):
        """Test that SELECT keyword is case-insensitive."""
        mock_response = {
            "data": {
                "id": "query-303",
                "nextUri": "",
                "stats": {"state": "FINISHED"},
                "columns": [{"name": "id", "type": "bigint"}],
                "data": [[1]],
            }
        }

        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        # Test various cases
        for query in [
            "select * from customers",
            "SELECT * FROM customers",
            "SeLeCt * FROM customers",
            "  SELECT * FROM customers",
        ]:
            result = await execute_select.fn(
                mock_context,
                sql=query,
                catalog_name="iceberg_data",
                schema_name="sales_db",
                engine_id="presto-01",
            )
            assert result["row_count"] == 1

    @pytest.mark.asyncio
    async def test_execute_select_empty_result(self, mock_context, watsonx_client, respx_mock):
        """Test query with empty result set."""
        mock_response = {
            "data": {
                "id": "query-404",
                "nextUri": "",
                "stats": {"state": "FINISHED"},
                "columns": [
                    {"name": "id", "type": "bigint"},
                    {"name": "name", "type": "varchar"},
                ],
                "data": [],
            }
        }

        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await execute_select.fn(
            mock_context,
            sql="SELECT id, name FROM customers WHERE id = -1",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        assert result["row_count"] == 0
        assert result["rows"] == []
        assert len(result["columns"]) == 2

    @pytest.mark.asyncio
    async def test_execute_select_with_null_values(self, mock_context, watsonx_client, respx_mock):
        """Test query result with NULL values."""
        mock_response = {
            "data": {
                "id": "query-505",
                "nextUri": "",
                "stats": {"state": "FINISHED"},
                "columns": [
                    {"name": "id", "type": "bigint"},
                    {"name": "name", "type": "varchar"},
                    {"name": "email", "type": "varchar"},
                ],
                "data": [[1, "John Doe", "john@example.com"], [2, "Jane Smith", None]],
            }
        }

        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await execute_select.fn(
            mock_context,
            sql="SELECT * FROM customers",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        assert result["row_count"] == 2
        assert result["rows"][1][2] is None

    @pytest.mark.asyncio
    async def test_execute_select_complex_query(self, mock_context, watsonx_client, respx_mock):
        """Test complex SELECT query with JOINs, GROUP BY, etc."""
        query = """
        SELECT
            c.id,
            c.name,
            COUNT(o.id) as order_count,
            SUM(o.total) as total_revenue
        FROM customers c
        LEFT JOIN orders o ON c.id = o.customer_id
        WHERE c.status = 'active'
        GROUP BY c.id, c.name
        HAVING COUNT(o.id) > 0
        ORDER BY total_revenue DESC
        LIMIT 10
        """

        mock_response = {
            "data": {
                "id": "query-606",
                "nextUri": "",
                "stats": {"state": "FINISHED"},
                "columns": [
                    {"name": "id", "type": "bigint"},
                    {"name": "name", "type": "varchar"},
                    {"name": "order_count", "type": "bigint"},
                    {"name": "total_revenue", "type": "decimal"},
                ],
                "data": [[1, "Customer A", 5, 7500.00]],
            }
        }

        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await execute_select.fn(
            mock_context,
            sql=query,
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        assert result["row_count"] == 1
        assert len(result["columns"]) == 4

    @pytest.mark.asyncio
    async def test_execute_select_polling_through_states(self, mock_context, watsonx_client, respx_mock):
        """Test polling through RUNNING to FINISHED states."""
        # First response: RUNNING state with data
        running_response = {
            "data": {
                "id": "query-707",
                "nextUri": "next-page-1",
                "stats": {"state": "RUNNING"},
                "columns": [{"name": "id", "type": "bigint"}],
                "data": [[1], [2], [3]],
            }
        }

        # Second response: FINISHED state (data may be empty)
        finished_response = {
            "data": {
                "id": "query-707",
                "nextUri": "",
                "stats": {"state": "FINISHED"},
                "columns": [],
                "data": [],
            }
        }

        route = respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01")
        route.side_effect = [
            httpx.Response(200, json=running_response),
            httpx.Response(200, json=finished_response),
        ]

        result = await execute_select.fn(
            mock_context,
            sql="SELECT * FROM customers",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        # Should capture data from RUNNING state
        assert result["row_count"] == 3
        assert len(result["rows"]) == 3
        assert result["query_id"] == "query-707"

    @pytest.mark.asyncio
    async def test_execute_select_data_captured_during_running(self, mock_context, watsonx_client, respx_mock):
        """Test that data is captured during RUNNING state before it disappears."""
        # First response: RUNNING with data
        running_response = {
            "data": {
                "id": "query-808",
                "nextUri": "next-1",
                "stats": {"state": "RUNNING"},
                "columns": [
                    {"name": "id", "type": "bigint"},
                    {"name": "name", "type": "varchar"},
                ],
                "data": [[1, "Alice"], [2, "Bob"], [3, "Charlie"]],
            }
        }

        # Second response: FINISHED without data (realistic behavior)
        finished_response = {
            "data": {
                "id": "query-808",
                "nextUri": "",
                "stats": {"state": "FINISHED"},
                "columns": [],
                "data": [],
            }
        }

        route = respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01")
        route.side_effect = [
            httpx.Response(200, json=running_response),
            httpx.Response(200, json=finished_response),
        ]

        result = await execute_select.fn(
            mock_context,
            sql="SELECT id, name FROM customers",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        # Verify data was captured from RUNNING state
        assert result["row_count"] == 3
        assert result["rows"] == [[1, "Alice"], [2, "Bob"], [3, "Charlie"]]
        assert len(result["columns"]) == 2

    @pytest.mark.asyncio
    async def test_execute_select_api_error(self, mock_context, watsonx_client, respx_mock):
        """Test query execution with API error."""
        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(400, json={"error": "Syntax error in query"})
        )

        with pytest.raises(httpx.HTTPStatusError):
            await execute_select.fn(
                mock_context,
                sql="SELECT * FROM nonexistent_table",
                catalog_name="iceberg_data",
                schema_name="sales_db",
                engine_id="presto-01",
            )

    @pytest.mark.asyncio
    async def test_execute_select_engine_not_running(self, mock_context, watsonx_client, respx_mock):
        """Test query execution when engine is not running."""
        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(400, json={"error": "Engine presto-01 is not running"})
        )

        with pytest.raises(httpx.HTTPStatusError):
            await execute_select.fn(
                mock_context,
                sql="SELECT 1",
                catalog_name="iceberg_data",
                schema_name="sales_db",
                engine_id="presto-01",
            )

    @pytest.mark.asyncio
    async def test_execute_select_timeout(self, mock_context, watsonx_client, respx_mock):
        """Test query execution timeout."""
        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            side_effect=httpx.TimeoutException("Query timed out")
        )

        with pytest.raises(httpx.TimeoutException):
            await execute_select.fn(
                mock_context,
                sql="SELECT * FROM large_table",
                catalog_name="iceberg_data",
                schema_name="sales_db",
                engine_id="presto-01",
            )

    @pytest.mark.asyncio
    async def test_execute_select_removes_trailing_semicolon_before_limit(self, mock_context, watsonx_client, respx_mock):
        """Test that trailing semicolon is removed before adding LIMIT."""
        mock_response = {
            "data": {
                "id": "query-909",
                "nextUri": "",
                "stats": {"state": "FINISHED"},
                "columns": [{"name": "id", "type": "bigint"}],
                "data": [[1]],
            }
        }

        route = respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        await execute_select.fn(
            mock_context,
            sql="SELECT * FROM customers;",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
            limit=10,
        )

        # Verify semicolon was removed before LIMIT was added
        request_body = route.calls[0].request.content
        # Should not have "; LIMIT 10", should have " LIMIT 10"
        assert b"; LIMIT" not in request_body
        assert b"LIMIT 10" in request_body

    @pytest.mark.asyncio
    async def test_execute_select_query_failed_state(self, mock_context, watsonx_client, respx_mock):
        """Test handling of FAILED query state."""
        failed_response = {
            "data": {
                "id": "query-1010",
                "nextUri": "",
                "stats": {"state": "FAILED"},
            },
            "error": {"message": "Table does not exist"},
        }

        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=failed_response)
        )

        with pytest.raises(RuntimeError) as exc_info:
            await execute_select.fn(
                mock_context,
                sql="SELECT * FROM nonexistent",
                catalog_name="iceberg_data",
                schema_name="sales_db",
                engine_id="presto-01",
            )

        assert "Query failed" in str(exc_info.value)
        assert "Table does not exist" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_select_query_canceled_state(self, mock_context, watsonx_client, respx_mock):
        """Test handling of CANCELED query state."""
        canceled_response = {
            "data": {
                "id": "query-1111",
                "nextUri": "",
                "stats": {"state": "CANCELED"},
            }
        }

        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=canceled_response)
        )

        with pytest.raises(RuntimeError) as exc_info:
            await execute_select.fn(
                mock_context,
                sql="SELECT * FROM customers",
                catalog_name="iceberg_data",
                schema_name="sales_db",
                engine_id="presto-01",
            )

        assert "Query was canceled" in str(exc_info.value)
