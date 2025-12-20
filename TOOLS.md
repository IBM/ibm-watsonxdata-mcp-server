# Tool Reference

Complete reference for all 6 read-only watsonx.data MCP tools.

## Table of Contents

- [Platform Tools](#platform-tools)
  - [get_instance_details](#get_instance_details)
- [Engine Tools](#engine-tools)
  - [list_engines](#list_engines)
- [Catalog Tools](#catalog-tools)
  - [list_schemas](#list_schemas)
  - [list_tables](#list_tables)
  - [describe_table](#describe_table)
- [Query Tools](#query-tools)
  - [execute_select](#execute_select)
- [Usage Patterns](#usage-patterns)
- [Best Practices](#best-practices)

---

## Platform Tools

### get_instance_details

Retrieve watsonx.data instance information including status, version, region, and enabled features.

**Category**: Platform Management

**Parameters**: None

**Returns**:
- `instance_id` (string): Unique instance identifier (CRN)
- `region` (string): Deployment region (e.g., "us-south")
- `version` (string): watsonx.data version
- `status` (string): Instance status ("active", "inactive", etc.)
- `features` (object): Enabled features (Serverless Spark, Presto, etc.)
- `metadata` (object): Additional configuration details

**Example Usage:**

**Natural language:**
```
What watsonx.data instance am I connected to?
```

**Claude responds:**
```
You're connected to watsonx.data instance:

Instance Details:
- ID: crn:v1:bluemix:public:lakehouse:us-south:a/abc123def456:instance-789
- Region: us-south
- Version: 2.0.0
- Status: active

Enabled Features:
✓ Presto engines
✓ Serverless Spark
✓ Iceberg tables
✓ Hive metastore

This instance is healthy and ready for queries.
```

**Use Cases:**
- Verify connectivity before starting work
- Check which features are available
- Confirm instance region and configuration
- Troubleshooting connection issues

---

## Engine Tools

### list_engines

List all available Presto and Spark engines with their status, configuration, and capabilities.

**Category**: Engine Management

**Parameters**:
- `engine_type` (string, optional): Filter by engine type
  - Values: `"presto"`, `"spark"`, or omit for all
  - Default: Returns both engine types

**Returns**:
- `engines` (array): List of engine objects
  - `engine_id` (string): Unique engine identifier
  - `name` (string): Human-readable engine name
  - `type` (string): "presto" or "spark"
  - `status` (string): "running", "stopped", "starting", etc.
  - `config` (object): Engine configuration details
  - `created_at` (string): Creation timestamp

**Example Usage:**

**Natural language:**
```
Show me all available engines
```

**Claude responds:**
```
Available watsonx.data Engines:

Presto Engines (2):
1. presto-01
   - Status: running
   - Version: 0.285
   - Nodes: 3
   - Coordinator: presto-01-coordinator.ibm.com:8080

2. presto-analytics
   - Status: running
   - Version: 0.285
   - Nodes: 5
   - Coordinator: presto-analytics-coordinator.ibm.com:8080

Spark Engines (1):
3. spark-serverless
   - Status: running
   - Version: 3.3.2
   - Mode: serverless
   - Auto-scaling: enabled

You can use any of these engines for queries by specifying the engine_id.
```

**Filter by type:**
```
List only Presto engines
```

**Claude responds:**
```
Presto Engines (2):

1. presto-01 (running)
   - 3 worker nodes
   - Optimized for interactive queries

2. presto-analytics (running)
   - 5 worker nodes
   - Optimized for analytical workloads
```

**Use Cases:**
- Find available engines before querying
- Check engine status and availability
- Select appropriate engine for workload
- Monitor engine health

**Best Practice:** Get engine IDs from this tool before using `execute_select`

---

## Catalog Tools

### list_schemas

Browse database schemas across all catalogs in your lakehouse.

**Category**: Catalog Management

**Parameters**:
- `catalog_name` (string, optional): Filter by specific catalog
  - Default: Returns schemas from all catalogs
- `engine_id` (string, optional): Use specific engine for metadata
  - Default: Uses default engine

**Returns**:
- `schemas` (array): List of schema objects
  - `schema_name` (string): Schema name
  - `catalog_name` (string): Parent catalog name
  - `table_count` (integer): Number of tables in schema
  - `location` (string): Storage location URI
  - `created_at` (string): Creation timestamp

**Example Usage:**

**List all schemas:**
```
What schemas are available in my data lakehouse?
```

**Claude responds:**
```
Available Schemas Across Catalogs:

iceberg_data catalog:
  • sales_db (15 tables)
    Location: s3://lakehouse/iceberg/sales_db
  • customer_db (8 tables)
    Location: s3://lakehouse/iceberg/customer_db
  • inventory_db (12 tables)
    Location: s3://lakehouse/iceberg/inventory_db

hive_data catalog:
  • logs_db (5 tables)
    Location: s3://lakehouse/hive/logs_db
  • analytics_db (20 tables)
    Location: s3://lakehouse/hive/analytics_db

Total: 5 schemas containing 60 tables
```

**Filter by catalog:**
```
List schemas in the iceberg_data catalog
```

**Claude responds:**
```
Schemas in iceberg_data catalog:

1. sales_db (15 tables)
   - Customer transactions and orders
   - Location: s3://lakehouse/iceberg/sales_db

2. customer_db (8 tables)
   - Customer profiles and segments
   - Location: s3://lakehouse/iceberg/customer_db

3. inventory_db (12 tables)
   - Product inventory and warehouses
   - Location: s3://lakehouse/iceberg/inventory_db
```

**Use Cases:**
- Discover available data before querying
- Understand data organization
- Find relevant schemas for analysis
- Data catalog exploration

---

### list_tables

List all tables within a specific schema with metadata and statistics.

**Category**: Catalog Management

**Parameters**:
- `catalog_name` (string, required): Catalog name
- `schema_name` (string, required): Schema name
- `engine_id` (string, optional): Engine to use for metadata
  - Default: Uses default engine

**Returns**:
- `tables` (array): List of table objects
  - `table_name` (string): Table name
  - `table_type` (string): "TABLE", "VIEW", "EXTERNAL_TABLE"
  - `row_count` (integer): Approximate row count
  - `size_bytes` (integer): Table size in bytes
  - `format` (string): Storage format (Parquet, ORC, etc.)
  - `partitioned` (boolean): Whether table is partitioned
  - `created_at` (string): Creation timestamp

**Example Usage:**

**List tables:**
```
Show me all tables in the sales_db schema
```

**Claude responds:**
```
Tables in iceberg_data.sales_db:

1. customers (TABLE)
   - Rows: ~1.2M
   - Size: 256 MB
   - Format: Parquet
   - Partitioned: Yes (by created_date)

2. orders (TABLE)
   - Rows: ~5.8M
   - Size: 1.2 GB
   - Format: Parquet
   - Partitioned: Yes (by order_date)

3. order_items (TABLE)
   - Rows: ~15.4M
   - Size: 892 MB
   - Format: Parquet
   - Partitioned: Yes (by order_date)

4. products (TABLE)
   - Rows: ~50K
   - Size: 12 MB
   - Format: Parquet
   - Partitioned: No

5. customer_segments (VIEW)
   - Type: View
   - Based on: customers table

Total: 5 objects (4 tables, 1 view)
```

**Use Cases:**
- Explore schema contents
- Find specific tables for querying
- Understand table sizes before queries
- Check partition information

**Best Practice:** Use this tool before `describe_table` to find table names

---

### describe_table

Get detailed schema information for a specific table including columns, data types, constraints, partitions, and statistics.

**Category**: Catalog Management

**Parameters**:
- `catalog_name` (string, required): Catalog name
- `schema_name` (string, required): Schema name
- `table_name` (string, required): Table name
- `engine_id` (string, optional): Engine to use for metadata
  - Default: Uses default engine

**Returns**:
- `table_info` (object): Table metadata
  - `columns` (array): Column definitions
    - `name` (string): Column name
    - `type` (string): Data type
    - `nullable` (boolean): Whether NULL values allowed
    - `comment` (string): Column description
  - `partitions` (array): Partition column definitions
  - `statistics` (object): Table statistics
    - `row_count` (integer): Total rows
    - `size_bytes` (integer): Storage size
    - `last_modified` (string): Last update timestamp
  - `properties` (object): Table properties and configuration

**Example Usage:**

**Describe table structure:**
```
Show me the schema for the customers table
```

**Claude responds:**
```
Table: iceberg_data.sales_db.customers

Columns (8):
  1. customer_id (BIGINT) NOT NULL
     - Primary identifier

  2. first_name (VARCHAR)
     - Customer first name

  3. last_name (VARCHAR)
     - Customer last name

  4. email (VARCHAR)
     - Contact email address

  5. phone (VARCHAR)
     - Phone number

  6. created_at (TIMESTAMP) NOT NULL
     - Account creation date

  7. lifetime_value (DECIMAL(10,2))
     - Total customer value in USD

  8. segment (VARCHAR)
     - Customer segment (gold/silver/bronze)

Partitioning:
  - Partitioned by: created_at (monthly partitions)
  - Partition count: 36 (3 years of data)

Statistics:
  - Total rows: 1,245,678
  - Table size: 256 MB
  - Last modified: 2024-01-15 10:30:00 UTC
  - Format: Parquet with Snappy compression

Table Properties:
  - write.format.default: parquet
  - write.metadata.compression-codec: gzip
  - commit.retry.num-retries: 4
```

**Use Cases:**
- Understand table structure before querying
- Check column data types for type conversions
- Identify partition columns for query optimization
- Verify column names for SQL queries
- Review table statistics

**Best Practice:** Always describe tables before writing complex queries

---

## Query Tools

### execute_select

Execute read-only SELECT queries against your lakehouse data with automatic safety validation.

**Category**: Query Execution

**Parameters**:
- `query` (string, required): SQL SELECT statement
  - Only SELECT queries allowed
  - Must be valid Presto/Spark SQL
- `engine_id` (string, required): Engine to execute query on
  - Get from `list_engines` tool
- `limit` (integer, optional): Maximum rows to return
  - Default: No limit (use with caution!)
  - Recommended: Always specify a limit

**Returns**:
- `columns` (array): Column names and types
- `rows` (array): Query result rows
- `row_count` (integer): Number of rows returned
- `execution_time_ms` (integer): Query duration
- `engine_used` (string): Engine that executed the query

**Safety Features**:
- ✅ Only SELECT statements allowed
- ❌ Automatically rejects: INSERT, UPDATE, DELETE, DROP, CREATE, ALTER
- ❌ Rejects: TRUNCATE, MERGE, GRANT, REVOKE
- ✅ Safe for production use

**Example Usage:**

**Simple query:**
```
Show me the top 10 customers by lifetime value
```

**Claude executes:**
```sql
SELECT customer_id, first_name, last_name, email, lifetime_value
FROM iceberg_data.sales_db.customers
ORDER BY lifetime_value DESC
LIMIT 10
```

**Results:**
```
Top 10 Customers by Lifetime Value:

| customer_id | first_name | last_name | email                    | lifetime_value |
|-------------|------------|-----------|--------------------------|----------------|
| 12847       | Sarah      | Johnson   | sarah.j@email.com        | $45,230.50     |
| 93021       | Michael    | Chen      | m.chen@business.com      | $42,180.25     |
| 45392       | Jennifer   | Williams  | jwilliams@corp.com       | $38,920.00     |
| 78234       | Robert     | Garcia    | rgarcia@company.com      | $35,445.75     |
| 23891       | Emily      | Martinez  | emily.m@email.com        | $32,190.50     |
| 67123       | David      | Anderson  | d.anderson@business.net  | $30,825.00     |
| 91042       | Lisa       | Taylor    | lisa.taylor@email.com    | $29,560.25     |
| 38475       | James      | Brown     | jbrown@corporate.com     | $28,340.00     |
| 52018       | Maria      | Rodriguez | maria.r@company.com      | $27,115.50     |
| 84639       | John       | Davis     | john.davis@business.com  | $26,890.75     |

Query executed in 1.2 seconds on engine: presto-01
```

**Complex query with aggregation:**
```
What were total sales by region last month?
```

**Claude executes:**
```sql
SELECT
  region,
  COUNT(*) as order_count,
  SUM(total_amount) as total_sales,
  AVG(total_amount) as avg_order_value
FROM iceberg_data.sales_db.orders
WHERE order_date >= DATE '2024-12-01'
  AND order_date < DATE '2025-01-01'
GROUP BY region
ORDER BY total_sales DESC
```

**Results:**
```
Sales by Region (December 2024):

| Region    | Orders | Total Sales | Avg Order Value |
|-----------|--------|-------------|-----------------|
| US-West   | 15,234 | $2,345,678  | $153.95         |
| US-East   | 14,892 | $2,187,543  | $146.89         |
| Europe    | 12,456 | $1,876,234  | $150.61         |
| Asia      | 10,983 | $1,654,321  | $150.62         |
| US-Central| 9,876  | $1,432,109  | $145.02         |

Total: 63,441 orders, $9,495,885 in sales
Query time: 2.8 seconds on engine: presto-analytics
```

**Safety validation examples:**

**Attempt unsafe query:**
```
DELETE FROM customers WHERE segment = 'inactive'
```

**Claude responds:**
```
❌ Error: This query cannot be executed.

Reason: DELETE operations are not allowed. This server only supports
read-only SELECT queries for safety.

If you need to modify data, please use the watsonx.data web console
or API directly with appropriate permissions.
```

**Use Cases:**
- Ad-hoc data analysis
- Business intelligence queries
- Data exploration and discovery
- Report generation
- Query prototyping

**Best Practices:**
- Always use LIMIT for large tables
- Use fully qualified names: `catalog.schema.table`
- Filter with WHERE clauses to reduce data scanned
- Query partitioned columns when possible
- Test queries with small limits first

---

## Usage Patterns

### Pattern 1: Data Discovery Workflow

Complete workflow for discovering and querying new data:

```
1. "What's my instance status?"
   → Verify connectivity

2. "List all engines"
   → Find available compute (get engine_id)

3. "What schemas are available?"
   → Discover catalogs and schemas

4. "Show tables in sales_db"
   → Find tables in interesting schema

5. "Describe the customers table"
   → Understand table structure

6. "SELECT * FROM customers LIMIT 10"
   → Query sample data
```

### Pattern 2: Targeted Analysis

When you know what you're looking for:

```
1. "List engines"
   → Get engine_id for queries

2. "Describe sales_db.orders table"
   → Review schema and partitions

3. "Show me orders from last month WHERE region='US-West'"
   → Execute filtered query with partition pruning
```

### Pattern 3: Schema Exploration

Understanding your data structure:

```
1. "List all schemas"
   → Get overview of data organization

2. "Show tables in each schema"
   → Explore schema contents

3. "Describe key tables"
   → Understand table structures

4. "Show sample data from each table"
   → Validate data content
```

### Pattern 4: Performance Investigation

Analyzing query performance:

```
1. "Describe table X"
   → Check if table is partitioned

2. "SELECT COUNT(*) FROM table WHERE partition_col = 'value'"
   → Test partition pruning

3. "SELECT * FROM table WHERE indexed_col = 'value' LIMIT 100"
   → Verify query performance on filtered data
```

### Pattern 5: Multi-Table Analysis

Working across multiple tables:

```
1. "List tables in sales_db"
   → Find related tables

2. "Describe customers and orders tables"
   → Understand join keys

3. "SELECT ... FROM customers c JOIN orders o ON c.customer_id = o.customer_id"
   → Execute join query
```

---

## Best Practices

### Query Performance

**DO:**
- ✅ Always use LIMIT for exploratory queries
- ✅ Filter on partition columns when available
- ✅ Use WHERE clauses to reduce data scanned
- ✅ Select only needed columns, not `SELECT *`
- ✅ Test queries with small limits before running on full data

**DON'T:**
- ❌ Run unlimited SELECT * on large tables
- ❌ Query without checking table size first
- ❌ Ignore partition columns in WHERE clauses
- ❌ Join large tables without filters

### Naming Conventions

**DO:**
- ✅ Use fully qualified names: `catalog.schema.table`
- ✅ Quote identifiers with special characters: `"table-name"`
- ✅ Use lowercase for consistency
- ✅ Get exact names from `list_schemas`/`list_tables`

**DON'T:**
- ❌ Assume schema names without checking
- ❌ Use unqualified table names
- ❌ Guess at column names

### Error Handling

**Common errors and solutions:**

**"Table not found":**
```
1. List schemas to verify catalog.schema exists
2. List tables to verify table exists
3. Use fully qualified name: catalog.schema.table
```

**"Engine not running":**
```
1. List engines to check status
2. Use a different engine_id
3. Wait for engine to start
```

**"Query timeout":**
```
1. Add LIMIT clause
2. Add WHERE filters
3. Use partition columns in WHERE
4. Check table size with describe_table
```

### Security & Safety

**Remember:**
- All operations are read-only
- No data modification possible
- No DDL operations allowed
- No administrative commands
- Safe for production use

**Credentials:**
- API keys are managed by server
- No need to handle authentication
- Automatic IAM token refresh
- TLS encryption for all API calls

---

