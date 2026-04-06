"""
Tests for query tools.

This file has been modified with the assistance of IBM Bob AI tool
"""

import httpx
import pytest

from lakehouse_mcp.tools.query.execute_insert import execute_insert
from lakehouse_mcp.tools.query.execute_select import execute_select
from lakehouse_mcp.tools.query.execute_update import execute_update


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

        result = await execute_select(
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

        await execute_select(
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

        await execute_select(
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

        await execute_select(
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
                await execute_select(
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
            await execute_select(
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
        result = await execute_select(
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
            result = await execute_select(
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

        result = await execute_select(
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

        result = await execute_select(
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

        result = await execute_select(
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

        result = await execute_select(
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

        result = await execute_select(
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
            return_value=httpx.Response(400, json={"message": "Syntax error in query"})
        )

        result = await execute_select(
            mock_context,
            sql="SELECT * FROM nonexistent_table",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )
        
        assert result["error"] is True
        assert "Syntax error in query" in result["error_message"]
        assert result["status_code"] == 400

    @pytest.mark.asyncio
    async def test_execute_select_engine_not_running(self, mock_context, watsonx_client, respx_mock):
        """Test query execution when engine is not running."""
        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(400, json={"message": "Engine presto-01 is not running"})
        )

        result = await execute_select(
            mock_context,
            sql="SELECT 1",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )
        
        assert result["error"] is True
        assert "Engine presto-01 is not running" in result["error_message"]
        assert result["status_code"] == 400

    @pytest.mark.asyncio
    async def test_execute_select_timeout(self, mock_context, watsonx_client, respx_mock):
        """Test query execution timeout."""
        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            side_effect=httpx.TimeoutException("Query timed out")
        )

        with pytest.raises(httpx.TimeoutException):
            await execute_select(
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

        await execute_select(
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

        result = await execute_select(
            mock_context,
            sql="SELECT * FROM nonexistent",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        assert result["error"] is True
        assert "Query failed" in result["error_message"]
        assert "Table does not exist" in result["error_message"]

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

        result = await execute_select(
            mock_context,
            sql="SELECT * FROM customers",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        assert result["error"] is True
        assert "Query was canceled" in result["error_message"]



class TestExecuteInsert:
    """Tests for execute_insert tool."""

    @pytest.mark.asyncio
    async def test_execute_insert_success(self, mock_context, watsonx_client, respx_mock):
        """Test successful INSERT execution."""
        mock_response = {
            "data": {
                "id": "query-insert-123",
                "nextUri": "",
                "stats": {"state": "FINISHED"},
                "updateCount": 3,
            }
        }

        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await execute_insert(
            mock_context,
            sql="INSERT INTO customers (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        assert result["query_id"] == "query-insert-123"
        assert result["rows_inserted"] == 3
        assert result["status"] == "success"
        assert result["catalog_name"] == "iceberg_data"
        assert result["schema_name"] == "sales_db"
        assert result["execution_time_ms"] >= 0

    @pytest.mark.asyncio
    async def test_execute_insert_invalid_query_not_insert(self, mock_context):
        """Test that non-INSERT queries are rejected."""
        invalid_queries = [
            "SELECT * FROM customers",
            "UPDATE customers SET name = 'test'",
            "DELETE FROM customers WHERE id = 1",
            "DROP TABLE customers",
            "CREATE TABLE test (id INT)",
        ]

        for query in invalid_queries:
            with pytest.raises(ValueError) as exc_info:
                await execute_insert(
                    mock_context,
                    sql=query,
                    catalog_name="iceberg_data",
                    schema_name="sales_db",
                    engine_id="presto-01",
                )

            assert "Only INSERT queries are allowed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_insert_multiple_statements_rejected(self, mock_context):
        """Test that multiple statements are rejected."""
        query = "INSERT INTO customers VALUES (1, 'test'); DROP TABLE customers;"

        with pytest.raises(ValueError) as exc_info:
            await execute_insert(
                mock_context,
                sql=query,
                catalog_name="iceberg_data",
                schema_name="sales_db",
                engine_id="presto-01",
            )

        assert "Multiple statements not allowed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_insert_trailing_semicolon_allowed(self, mock_context, watsonx_client, respx_mock):
        """Test that a single trailing semicolon is allowed."""
        mock_response = {
            "data": {
                "id": "query-insert-456",
                "nextUri": "",
                "stats": {"state": "FINISHED"},
                "updateCount": 1,
            }
        }

        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await execute_insert(
            mock_context,
            sql="INSERT INTO customers (id, name) VALUES (1, 'Test');",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        assert result["rows_inserted"] == 1

    @pytest.mark.asyncio
    async def test_execute_insert_case_insensitive(self, mock_context, watsonx_client, respx_mock):
        """Test that INSERT keyword is case-insensitive."""
        mock_response = {
            "data": {
                "id": "query-insert-789",
                "nextUri": "",
                "stats": {"state": "FINISHED"},
                "updateCount": 1,
            }
        }

        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        for query in [
            "insert into customers values (1, 'test')",
            "INSERT INTO customers VALUES (1, 'test')",
            "InSeRt INTO customers VALUES (1, 'test')",
        ]:
            result = await execute_insert(
                mock_context,
                sql=query,
                catalog_name="iceberg_data",
                schema_name="sales_db",
                engine_id="presto-01",
            )
            assert result["rows_inserted"] == 1

    @pytest.mark.asyncio
    async def test_execute_insert_empty_query(self, mock_context):
        """Test that empty queries are rejected."""
        with pytest.raises(ValueError) as exc_info:
            await execute_insert(
                mock_context,
                sql="",
                catalog_name="iceberg_data",
                schema_name="sales_db",
                engine_id="presto-01",
            )

        assert "SQL query cannot be empty" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_insert_polling_through_states(self, mock_context, watsonx_client, respx_mock):
        """Test polling through RUNNING to FINISHED states."""
        running_response = {
            "data": {
                "id": "query-insert-running",
                "nextUri": "next-page-1",
                "stats": {"state": "RUNNING"},
            }
        }

        finished_response = {
            "data": {
                "id": "query-insert-running",
                "nextUri": "",
                "stats": {"state": "FINISHED"},
                "updateCount": 5,
            }
        }

        route = respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01")
        route.side_effect = [
            httpx.Response(200, json=running_response),
            httpx.Response(200, json=finished_response),
        ]

        result = await execute_insert(
            mock_context,
            sql="INSERT INTO customers SELECT * FROM temp_customers",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        assert result["rows_inserted"] == 5
        assert result["query_id"] == "query-insert-running"

    @pytest.mark.asyncio
    async def test_execute_insert_query_failed_state(self, mock_context, watsonx_client, respx_mock):
        """Test handling of FAILED query state."""
        failed_response = {
            "data": {
                "id": "query-insert-failed",
                "nextUri": "",
                "stats": {"state": "FAILED"},
            },
            "error": {"message": "Table does not exist"},
        }

        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=failed_response)
        )

        result = await execute_insert(
            mock_context,
            sql="INSERT INTO nonexistent VALUES (1, 'test')",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        assert result["error"] is True
        assert "INSERT query failed" in result["error_message"]
        assert "Table does not exist" in result["error_message"]

    @pytest.mark.asyncio
    async def test_execute_insert_query_canceled_state(self, mock_context, watsonx_client, respx_mock):
        """Test handling of CANCELED query state."""
        canceled_response = {
            "data": {
                "id": "query-insert-canceled",
                "nextUri": "",
                "stats": {"state": "CANCELED"},
            }
        }

        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=canceled_response)
        )

        result = await execute_insert(
            mock_context,
            sql="INSERT INTO customers VALUES (1, 'test')",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        assert result["error"] is True
        assert "INSERT query was canceled" in result["error_message"]

    @pytest.mark.asyncio
    async def test_execute_insert_api_error(self, mock_context, watsonx_client, respx_mock):
        """Test INSERT execution with API error."""
        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(400, json={"message": "Syntax error in query"})
        )

        result = await execute_insert(
            mock_context,
            sql="INSERT INTO customers VALUES (1, 'test')",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        assert result["error"] is True
        assert "Syntax error in query" in result["error_message"]
        assert result["status_code"] == 400


class TestExecuteUpdate:
    """Tests for execute_update tool."""

    @pytest.mark.asyncio
    async def test_execute_update_success(self, mock_context, watsonx_client, respx_mock):
        """Test successful UPDATE execution."""
        mock_response = {
            "data": {
                "id": "query-update-123",
                "nextUri": "",
                "stats": {"state": "FINISHED"},
                "updateCount": 5,
            }
        }

        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await execute_update(
            mock_context,
            sql="UPDATE customers SET status = 'active' WHERE id > 100",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        assert result["query_id"] == "query-update-123"
        assert result["rows_updated"] == 5
        assert result["status"] == "success"
        assert result["catalog_name"] == "iceberg_data"
        assert result["schema_name"] == "sales_db"
        assert result["execution_time_ms"] >= 0

    @pytest.mark.asyncio
    async def test_execute_update_invalid_query_not_update(self, mock_context):
        """Test that non-UPDATE queries are rejected."""
        invalid_queries = [
            "SELECT * FROM customers",
            "INSERT INTO customers VALUES (1, 'test')",
            "DELETE FROM customers WHERE id = 1",
            "DROP TABLE customers",
            "CREATE TABLE test (id INT)",
        ]

        for query in invalid_queries:
            with pytest.raises(ValueError) as exc_info:
                await execute_update(
                    mock_context,
                    sql=query,
                    catalog_name="iceberg_data",
                    schema_name="sales_db",
                    engine_id="presto-01",
                )

            assert "Only UPDATE queries are allowed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_update_multiple_statements_rejected(self, mock_context):
        """Test that multiple statements are rejected."""
        query = "UPDATE customers SET name = 'test'; DROP TABLE customers;"

        with pytest.raises(ValueError) as exc_info:
            await execute_update(
                mock_context,
                sql=query,
                catalog_name="iceberg_data",
                schema_name="sales_db",
                engine_id="presto-01",
            )

        assert "Multiple statements not allowed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_update_trailing_semicolon_allowed(self, mock_context, watsonx_client, respx_mock):
        """Test that a single trailing semicolon is allowed."""
        mock_response = {
            "data": {
                "id": "query-update-456",
                "nextUri": "",
                "stats": {"state": "FINISHED"},
                "updateCount": 2,
            }
        }

        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await execute_update(
            mock_context,
            sql="UPDATE customers SET name = 'Test' WHERE id = 1;",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        assert result["rows_updated"] == 2

    @pytest.mark.asyncio
    async def test_execute_update_case_insensitive(self, mock_context, watsonx_client, respx_mock):
        """Test that UPDATE keyword is case-insensitive."""
        mock_response = {
            "data": {
                "id": "query-update-789",
                "nextUri": "",
                "stats": {"state": "FINISHED"},
                "updateCount": 1,
            }
        }

        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        for query in [
            "update customers set name = 'test' where id = 1",
            "UPDATE customers SET name = 'test' WHERE id = 1",
            "UpDaTe customers SET name = 'test' WHERE id = 1",
        ]:
            result = await execute_update(
                mock_context,
                sql=query,
                catalog_name="iceberg_data",
                schema_name="sales_db",
                engine_id="presto-01",
            )
            assert result["rows_updated"] == 1

    @pytest.mark.asyncio
    async def test_execute_update_empty_query(self, mock_context):
        """Test that empty queries are rejected."""
        with pytest.raises(ValueError) as exc_info:
            await execute_update(
                mock_context,
                sql="",
                catalog_name="iceberg_data",
                schema_name="sales_db",
                engine_id="presto-01",
            )

        assert "SQL query cannot be empty" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_update_polling_through_states(self, mock_context, watsonx_client, respx_mock):
        """Test polling through RUNNING to FINISHED states."""
        running_response = {
            "data": {
                "id": "query-update-running",
                "nextUri": "next-page-1",
                "stats": {"state": "RUNNING"},
            }
        }

        finished_response = {
            "data": {
                "id": "query-update-running",
                "nextUri": "",
                "stats": {"state": "FINISHED"},
                "updateCount": 10,
            }
        }

        route = respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01")
        route.side_effect = [
            httpx.Response(200, json=running_response),
            httpx.Response(200, json=finished_response),
        ]

        result = await execute_update(
            mock_context,
            sql="UPDATE customers SET status = 'inactive' WHERE last_login < '2020-01-01'",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        assert result["rows_updated"] == 10
        assert result["query_id"] == "query-update-running"

    @pytest.mark.asyncio
    async def test_execute_update_query_failed_state(self, mock_context, watsonx_client, respx_mock):
        """Test handling of FAILED query state."""
        failed_response = {
            "data": {
                "id": "query-update-failed",
                "nextUri": "",
                "stats": {"state": "FAILED"},
            },
            "error": {"message": "Column does not exist"},
        }

        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=failed_response)
        )

        result = await execute_update(
            mock_context,
            sql="UPDATE customers SET nonexistent_column = 'test'",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        assert result["error"] is True
        assert "UPDATE query failed" in result["error_message"]
        assert "Column does not exist" in result["error_message"]

    @pytest.mark.asyncio
    async def test_execute_update_query_canceled_state(self, mock_context, watsonx_client, respx_mock):
        """Test handling of CANCELED query state."""
        canceled_response = {
            "data": {
                "id": "query-update-canceled",
                "nextUri": "",
                "stats": {"state": "CANCELED"},
            }
        }

        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=canceled_response)
        )

        result = await execute_update(
            mock_context,
            sql="UPDATE customers SET name = 'test'",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        assert result["error"] is True
        assert "UPDATE query was canceled" in result["error_message"]

    @pytest.mark.asyncio
    async def test_execute_update_api_error(self, mock_context, watsonx_client, respx_mock):
        """Test UPDATE execution with API error."""
        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(400, json={"message": "Syntax error in query"})
        )

        result = await execute_update(
            mock_context,
            sql="UPDATE customers SET name = 'test'",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        assert result["error"] is True
        assert "Syntax error in query" in result["error_message"]
        assert result["status_code"] == 400

    @pytest.mark.asyncio
    async def test_execute_update_zero_rows_updated(self, mock_context, watsonx_client, respx_mock):
        """Test UPDATE with no matching rows."""
        mock_response = {
            "data": {
                "id": "query-update-zero",
                "nextUri": "",
                "stats": {"state": "FINISHED"},
                "updateCount": 0,
            }
        }

        respx_mock.post("https://test.watsonx.com/api/v3/v1/statement?engine_id=presto-01").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await execute_update(
            mock_context,
            sql="UPDATE customers SET name = 'test' WHERE id = -999",
            catalog_name="iceberg_data",
            schema_name="sales_db",
            engine_id="presto-01",
        )

        assert result["rows_updated"] == 0
        assert result["status"] == "success"


class TestExplainQuery:
    """Tests for explain_query tool."""

    @pytest.mark.asyncio
    async def test_explain_query_presto_success(self, mock_context, watsonx_client, respx_mock):
        """Test successful query explanation for Presto engine."""
        from lakehouse_mcp.tools.query.explain_query import explain_query

        mock_response = {
            "result": "Fragment 0 [SINGLE]\n    Output layout: [id, name]\n    Output partitioning: SINGLE\n"
        }

        respx_mock.post("https://test.watsonx.com/api/v3/presto_engines/presto-01/query_explain").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await explain_query(
            mock_context,
            engine_id="presto-01",
            statement="SELECT id, name FROM customers",
            engine_type="presto",
        )

        assert result["engine_id"] == "presto-01"
        assert result["engine_type"] == "presto"
        assert result["statement"] == "SELECT id, name FROM customers"
        assert "Fragment 0" in result["plan"]

    @pytest.mark.asyncio
    async def test_explain_query_prestissimo_success(self, mock_context, watsonx_client, respx_mock):
        """Test successful query explanation for Prestissimo engine."""
        from lakehouse_mcp.tools.query.explain_query import explain_query

        mock_response = {
            "result": "Fragment 0 [SINGLE]\n    Output layout: [id, name]\n    Output partitioning: SINGLE\n"
        }

        respx_mock.post("https://test.watsonx.com/api/v3/prestissimo_engines/prestissimo-01/query_explain").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await explain_query(
            mock_context,
            engine_id="prestissimo-01",
            statement="SELECT id, name FROM customers",
            engine_type="prestissimo",
        )

        assert result["engine_id"] == "prestissimo-01"
        assert result["engine_type"] == "prestissimo"
        assert "Fragment 0" in result["plan"]

    @pytest.mark.asyncio
    async def test_explain_query_with_format_json(self, mock_context, watsonx_client, respx_mock):
        """Test query explanation with JSON format."""
        from lakehouse_mcp.tools.query.explain_query import explain_query

        mock_response = {
            "result": '{"fragments": [{"id": 0, "type": "SINGLE"}]}'
        }

        route = respx_mock.post("https://test.watsonx.com/api/v3/presto_engines/presto-01/query_explain").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await explain_query(
            mock_context,
            engine_id="presto-01",
            statement="SELECT * FROM customers",
            engine_type="presto",
            format="json",
        )

        # Verify format parameter was sent
        request_body = route.calls[0].request.content
        assert b'"format":"json"' in request_body
        assert "fragments" in result["plan"]

    @pytest.mark.asyncio
    async def test_explain_query_with_type_distributed(self, mock_context, watsonx_client, respx_mock):
        """Test query explanation with distributed type."""
        from lakehouse_mcp.tools.query.explain_query import explain_query

        mock_response = {
            "result": "Distributed plan:\nFragment 0 [SOURCE]\n  TableScan[customers]\n"
        }

        route = respx_mock.post("https://test.watsonx.com/api/v3/presto_engines/presto-01/query_explain").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await explain_query(
            mock_context,
            engine_id="presto-01",
            statement="SELECT * FROM customers",
            engine_type="presto",
            type="distributed",
        )

        # Verify type parameter was sent
        request_body = route.calls[0].request.content
        assert b'"type":"distributed"' in request_body
        assert "Distributed plan" in result["plan"]

    @pytest.mark.asyncio
    async def test_explain_query_invalid_engine_type(self, mock_context):
        """Test that invalid engine type is rejected."""
        from lakehouse_mcp.tools.query.explain_query import explain_query

        with pytest.raises(ValueError) as exc_info:
            await explain_query(
                mock_context,
                engine_id="spark-01",
                statement="SELECT * FROM customers",
                engine_type="spark",  # Invalid - only presto/prestissimo supported
            )

        assert "Invalid engine_type" in str(exc_info.value)
        assert "Must be 'presto' or 'prestissimo'" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_explain_query_api_error(self, mock_context, watsonx_client, respx_mock):
        """Test query explanation with API error."""
        from lakehouse_mcp.tools.query.explain_query import explain_query

        respx_mock.post("https://test.watsonx.com/api/v3/presto_engines/presto-01/query_explain").mock(
            return_value=httpx.Response(400, json={"message": "Invalid SQL syntax"})
        )

        result = await explain_query(
            mock_context,
            engine_id="presto-01",
            statement="SELECT * FORM customers",  # Typo: FORM instead of FROM
            engine_type="presto",
        )
        
        assert result["error"] is True
        assert "Invalid SQL syntax" in result["error_message"]
        assert result["status_code"] == 400


class TestExplainAnalyzeQuery:
    """Tests for explain_analyze_query tool."""

    @pytest.mark.asyncio
    async def test_explain_analyze_query_presto_success(self, mock_context, watsonx_client, respx_mock):
        """Test successful query analysis for Presto engine."""
        from lakehouse_mcp.tools.query.explain_analyze_query import explain_analyze_query

        mock_response = {
            "result": "Fragment 0 [SINGLE]\n    CPU: 1.23s, Scheduled: 2.45s, Input: 1000 rows\n"
        }

        respx_mock.post("https://test.watsonx.com/api/v3/presto_engines/presto-01/query_explain_analyze").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await explain_analyze_query(
            mock_context,
            engine_id="presto-01",
            statement="SELECT id, name FROM customers",
            engine_type="presto",
        )

        assert result["engine_id"] == "presto-01"
        assert result["engine_type"] == "presto"
        assert result["statement"] == "SELECT id, name FROM customers"
        assert "CPU:" in result["analysis"]

    @pytest.mark.asyncio
    async def test_explain_analyze_query_prestissimo_success(self, mock_context, watsonx_client, respx_mock):
        """Test successful query analysis for Prestissimo engine."""
        from lakehouse_mcp.tools.query.explain_analyze_query import explain_analyze_query

        mock_response = {
            "result": "Fragment 0 [SINGLE]\n    CPU: 0.89s, Scheduled: 1.23s, Input: 500 rows\n"
        }

        respx_mock.post("https://test.watsonx.com/api/v3/prestissimo_engines/prestissimo-01/query_explain_analyze").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await explain_analyze_query(
            mock_context,
            engine_id="prestissimo-01",
            statement="SELECT id, name FROM customers",
            engine_type="prestissimo",
        )

        assert result["engine_id"] == "prestissimo-01"
        assert result["engine_type"] == "prestissimo"
        assert "CPU:" in result["analysis"]

    @pytest.mark.asyncio
    async def test_explain_analyze_query_with_verbose(self, mock_context, watsonx_client, respx_mock):
        """Test query analysis with verbose flag."""
        from lakehouse_mcp.tools.query.explain_analyze_query import explain_analyze_query

        mock_response = {
            "result": "Detailed analysis:\nFragment 0 [SINGLE]\n    CPU: 1.23s\n    Memory: 256MB\n"
        }

        route = respx_mock.post("https://test.watsonx.com/api/v3/presto_engines/presto-01/query_explain_analyze").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = await explain_analyze_query(
            mock_context,
            engine_id="presto-01",
            statement="SELECT * FROM customers",
            engine_type="presto",
            verbose=True,
        )

        # Verify verbose parameter was sent
        request_body = route.calls[0].request.content
        assert b'"verbose":true' in request_body
        assert "Detailed analysis" in result["analysis"]

    @pytest.mark.asyncio
    async def test_explain_analyze_query_invalid_engine_type(self, mock_context):
        """Test that invalid engine type is rejected."""
        from lakehouse_mcp.tools.query.explain_analyze_query import explain_analyze_query

        with pytest.raises(ValueError) as exc_info:
            await explain_analyze_query(
                mock_context,
                engine_id="spark-01",
                statement="SELECT * FROM customers",
                engine_type="spark",  # Invalid - only presto/prestissimo supported
            )

        assert "Invalid engine_type" in str(exc_info.value)
        assert "Must be 'presto' or 'prestissimo'" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_explain_analyze_query_api_error(self, mock_context, watsonx_client, respx_mock):
        """Test query analysis with API error."""
        from lakehouse_mcp.tools.query.explain_analyze_query import explain_analyze_query

        respx_mock.post("https://test.watsonx.com/api/v3/presto_engines/presto-01/query_explain_analyze").mock(
            return_value=httpx.Response(400, json={"message": "Query execution failed"})
        )

        result = await explain_analyze_query(
            mock_context,
            engine_id="presto-01",
            statement="SELECT * FROM nonexistent_table",
            engine_type="presto",
        )
        
        assert result["error"] is True
        assert "Query execution failed" in result["error_message"]
        assert result["status_code"] == 400
