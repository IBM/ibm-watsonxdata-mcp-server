# Tool Reference

Complete reference for all watsonx.data MCP tools.

## Table of Contents

- [Platform Tools](#platform-tools)
  - [get_instance_details](#get_instance_details)
- [Engine Tools](#engine-tools)
  - [list_engines](#list_engines)
  - [create_spark_engine](#create_spark_engine)
  - [update_spark_engine](#update_spark_engine)
  - [pause_spark_engine](#pause_spark_engine)
  - [resume_spark_engine](#resume_spark_engine)
- [Catalog Tools](#catalog-tools)
  - [list_schemas](#list_schemas)
  - [list_tables](#list_tables)
  - [describe_table](#describe_table)
  - [create_schema](#create_schema)
  - [rename_table](#rename_table)
  - [add_columns](#add_columns)
  - [rename_column](#rename_column)
- [Query Tools](#query-tools)
  - [execute_select](#execute_select)
  - [execute_insert](#execute_insert)
  - [execute_update](#execute_update)
  - [explain_query](#explain_query)
  - [explain_analyze_query](#explain_analyze_query)
- [Spark Application Tools](#spark-application-tools)
  - [submit_spark_application](#submit_spark_application)
  - [list_spark_applications](#list_spark_applications)
  - [get_spark_application_status](#get_spark_application_status)
  - [stop_spark_application](#stop_spark_application)
- [Data Ingestion Tools](#data-ingestion-tools)
  - [create_ingestion_job](#create_ingestion_job)
  - [list_ingestion_jobs](#list_ingestion_jobs)
  - [get_ingestion_job](#get_ingestion_job)
  - [delete_ingestion_job](#delete_ingestion_job)
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


### create_spark_engine

Create a new Spark engine in watsonx.data.

**Category**: Engine Management

**Parameters**:
- `origin` (string, required): Engine origin - "native", "external", or "discover"
- `display_name` (string, required): Display name for the engine
- `storage_name` (string, required): Storage/bucket name for engine_home
- `associated_catalogs` (array of strings, optional): List of catalog IDs to associate
- `description` (string, optional): Engine description
- `default_version` (string, optional): Spark version (default: "3.5")
- `default_config` (object, optional): Additional engine configuration
- `tags` (array of strings, optional): Tags for the engine

**Returns**:
- `engine_id` (string): Created engine identifier
- `display_name` (string): Engine display name
- `origin` (string): Engine origin
- `status` (string): Initial engine status

**Example Usage:**
```
Create a new Spark engine named spark-etl for data processing
```

**Use Cases:**
- Set up Spark engines for ETL workloads
- Create dedicated engines for machine learning
- Provision engines for batch processing
- Set up development/test Spark environments

---

### update_spark_engine

Update Spark engine configuration and settings.

**Category**: Engine Management

**Parameters**:
- `engine_id` (string, required): Spark engine identifier
- `description` (string, optional): Updated description
- `display_name` (string, optional): Updated display name
- `configuration` (object, optional): Engine configuration (default_config, default_version, engine_home)
- `tags` (array of strings, optional): Updated tags

**Returns**:
- Updated engine configuration

**Example Usage:**
```
Update spark-prod engine description and add production tag
```

**Note**: Spark engines do NOT support engine_restart parameter. Configuration changes may require manual restart.

---

### pause_spark_engine

Pause a running Spark engine (SAAS deployments only).

**Category**: Engine Lifecycle Management

**Parameters**:
- `engine_id` (string, required): Spark engine identifier
- `force` (boolean, optional): Force pause even if applications running (default: false)

**Returns**:
- `engine_id` (string): Engine identifier
- `forced` (boolean): Whether force flag was used
- `message` (string): Operation status message

**Example Usage:**
```
Pause spark-etl engine with force to stop running applications
```

**Use Cases:**
- Stop Spark engines during off-hours
- Force stop engines with stuck applications
- Save resources on SAAS deployments

**Important**: 
- **SAAS ONLY** - Not available in Software deployments
- `force=true` will terminate running applications
- `force=false` will fail if applications are running

---

### resume_spark_engine

Resume a paused Spark engine (SAAS deployments only).

**Category**: Engine Lifecycle Management

**Parameters**:
- `engine_id` (string, required): Spark engine identifier

**Returns**:
- `engine_id` (string): Engine identifier
- `message` (string): Operation status message

**Example Usage:**
```
Resume spark-etl for daily batch jobs
```

**Important**: **SAAS ONLY** - Not available in Software deployments

---
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

### create_schema

Create a new schema in a watsonx.data catalog with optional storage configuration.

**Category**: Catalog Management

**Parameters**:
- `catalog_id` (string, required): Catalog identifier (e.g., "iceberg_data")
- `schema_name` (string, required): Name for the new schema
- `engine_id` (string, required): Engine ID to use for the operation
- `custom_path` (string, optional): Path within bucket where schema will be created
- `storage_name` (string, optional): Storage/bucket name for the schema

**Returns**:
- `schema_name` (string): Name of the created schema
- `catalog_name` (string): Parent catalog name
- `custom_path` (string): Custom path if specified
- `storage_name` (string): Storage name if specified

**Example Usage:**

**Natural language:**
```
Create a new schema called analytics_prod in the iceberg_data catalog
```

**Claude responds:**
```
Creating new schema...

Schema Created Successfully:
- Schema name: analytics_prod
- Catalog: iceberg_data
- Engine used: presto-01

The schema is now ready for table creation. You can start creating
tables in iceberg_data.analytics_prod.
```

**With custom storage:**
```
Create a schema called sales_archive in iceberg_data with custom path /archive/sales
```

**Claude responds:**
```
Creating schema with custom storage configuration...

Schema Created:
- Schema name: sales_archive
- Catalog: iceberg_data
- Custom path: /archive/sales
- Storage location: s3://lakehouse-bucket/archive/sales

The schema has been created with a dedicated storage path for
archived sales data.
```

**Use Cases:**
- Set up new data domains
- Organize data by business unit
- Create isolated environments (dev/test/prod)
- Separate data by retention policies
- Implement data governance boundaries

**Best Practices:**
- Use descriptive schema names that reflect data purpose
- Consider storage location for data locality
- Plan schema structure before creating tables
- Document schema purpose and ownership
- Follow naming conventions consistently

### rename_table

Rename a table in a watsonx.data schema.

**Category**: Catalog Management

**Parameters**:
- `catalog_name` (string, required): Catalog containing the table (e.g., "iceberg_data")
- `schema_name` (string, required): Schema containing the table
- `table_name` (string, required): Current table name
- `new_table_name` (string, required): New name for the table
- `engine_id` (string, required): Engine ID to use for the operation (from list_engines)

**Returns**:
- `name` (string): New table name
- `catalog_name` (string): Catalog name
- `schema_name` (string): Schema name

**Example Usage:**

**Rename a table:**
```
Rename the customers table to customers_v2 in sales_db schema
```

**Claude responds:**
```
✓ Table renamed successfully

Old name: iceberg_data.sales_db.customers
New name: iceberg_data.sales_db.customers_v2

Note: Update any queries or applications that reference this table.
```

**Use Cases:**
- Implement table versioning (e.g., customers_v1 → customers_v2)
- Fix naming mistakes or typos
- Align with new naming conventions
- Prepare for table migrations

**Important Notes:**
- Table rename is atomic - no data is moved
- Existing queries using old name will fail
- Update dependent views and applications
- Cannot rename to an existing table name

**Best Practices:**
- Communicate table renames to team members
- Update documentation and data catalogs
- Check for dependent views before renaming
- Consider creating a view with old name for backward compatibility

---

### add_columns

Add one or more columns to a table in a watsonx.data schema.

**Category**: Catalog Management

**Parameters**:
- `catalog_name` (string, required): Catalog containing the table (e.g., "iceberg_data")
- `schema_name` (string, required): Schema containing the table
- `table_name` (string, required): Table to add columns to
- `columns` (array, required): List of column definitions, each with:
  - `name` (string, required): Column name
  - `type` (string, required): Data type (e.g., "string", "int", "decimal(10,2)")
  - `comment` (string, optional): Column description
  - `extra` (string, optional): Additional column properties
  - `precision` (integer, optional): Precision for decimal types
  - `scale` (integer, optional): Scale for decimal types
- `engine_id` (string, required): Engine ID to use for the operation (from list_engines)

**Returns**:
- `columns` (array): List of added column details
- `total_count` (integer): Number of columns added

**Example Usage:**

**Add single column:**
```
Add a column called loyalty_points of type int to the customers table
```

**Claude responds:**
```
✓ Column added successfully

Table: iceberg_data.sales_db.customers
New column: loyalty_points (INT)

The column has been added and is available for use.
Existing rows will have NULL values for this column.
```

**Add multiple columns:**
```
Add columns email_verified (boolean) and last_login (timestamp) to customers table
```

**Claude responds:**
```
✓ 2 columns added successfully

Table: iceberg_data.sales_db.customers

New columns:
  1. email_verified (BOOLEAN)
  2. last_login (TIMESTAMP)

Existing rows will have NULL values for these columns.
```

**Add decimal column with precision:**
```
Add a discount_rate column with decimal(5,2) type to products table
```

**Use Cases:**
- Extend table schema with new attributes
- Add tracking columns (created_at, updated_at)
- Implement feature flags or status columns
- Support new business requirements

**Important Notes:**
- New columns are added at the end of the table
- Existing rows will have NULL values for new columns
- Cannot add columns with NOT NULL constraint directly
- Column names must be unique within the table

**Best Practices:**
- Add descriptive comments to document column purpose
- Use appropriate data types for storage efficiency
- Consider default values for new columns
- Test queries after schema changes
- Document schema evolution in version control

---

### rename_column

Rename a column in a table in a watsonx.data schema.

**Category**: Catalog Management

**Parameters**:
- `catalog_name` (string, required): Catalog containing the table (e.g., "iceberg_data")
- `schema_name` (string, required): Schema containing the table
- `table_name` (string, required): Table containing the column
- `column_name` (string, required): Current column name
- `new_column_name` (string, required): New name for the column
- `engine_id` (string, required): Engine ID to use for the operation (from list_engines)

**Returns**:
- `name` (string): New column name
- `type` (string): Column data type
- `nullable` (boolean): Whether column allows NULL values

**Example Usage:**

**Rename a column:**
```
Rename the email column to customer_email in the customers table
```

**Claude responds:**
```
✓ Column renamed successfully

Table: iceberg_data.sales_db.customers
Old name: email
New name: customer_email
Type: VARCHAR

Note: Update any queries that reference this column.
```

**Use Cases:**
- Fix naming mistakes or typos
- Improve column name clarity
- Align with naming conventions
- Resolve naming conflicts

**Important Notes:**
- Column rename is metadata-only - no data is modified
- Existing queries using old name will fail
- Update dependent views and applications
- Cannot rename to an existing column name

**Best Practices:**
- Communicate column renames to team members
- Update documentation and data dictionaries
- Search codebase for references to old column name
- Consider impact on BI tools and reports
- Test queries after renaming

---

**Note:** Requires `lakehouse.schema.create` permission

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

### execute_insert

Execute INSERT statements to add new rows to tables in watsonx.data.

**Category**: Query Execution

**Parameters**:
- `sql` (string, required): SQL INSERT statement to execute
  - Must start with `INSERT`
  - Single statement only (no semicolons separating multiple statements)
- `catalog_name` (string, required): Target catalog (e.g., `"iceberg_data"`, `"hive_data"`)
- `schema_name` (string, required): Default schema for unqualified table names
- `engine_id` (string, required): Engine to run the query on (from `list_engines`, must be running)

**Returns**:
- `query_id` (string): Unique query identifier
- `rows_inserted` (integer): Number of rows inserted (if available)
- `execution_time_ms` (integer): Query duration in milliseconds
- `status` (string): Execution status (`"success"`)
- `catalog_name` (string): Echo of input catalog
- `schema_name` (string): Echo of input schema

**Safety Features**:
- ✅ Only INSERT statements allowed
- ❌ Rejects any query not starting with INSERT
- ❌ Rejects multiple statements separated by semicolons
- ⏱ 120-second execution timeout

**Example Usage:**

**Natural language:**
```
Insert a new customer record: id=9001, name='Alice Smith', email='alice@example.com', region='US-West'
```

**Claude executes:**
```sql
INSERT INTO iceberg_data.sales_db.customers (customer_id, name, email, region)
VALUES (9001, 'Alice Smith', 'alice@example.com', 'US-West')
```

**Claude responds:**
```
✓ INSERT executed successfully

Rows inserted: 1
Query ID: q-20250101-001
Execution time: 340ms
```

**Use Cases:**
- Add new records to Iceberg tables
- Populate staging tables for ETL pipelines
- Seed reference/lookup tables
- Append data during data migration

**Best Practices:**
- Use fully qualified table names: `catalog.schema.table`
- Verify the target table exists with `describe_table` first
- Keep INSERT batches reasonably sized to avoid timeouts
- Prefer INSERT INTO ... SELECT for bulk loads

---

### execute_update

Execute UPDATE statements to modify existing rows in tables in watsonx.data.

**Category**: Query Execution

**Parameters**:
- `sql` (string, required): SQL UPDATE statement to execute
  - Must start with `UPDATE`
  - Single statement only (no semicolons separating multiple statements)
- `catalog_name` (string, required): Target catalog (e.g., `"iceberg_data"`, `"hive_data"`)
- `schema_name` (string, required): Default schema for unqualified table names
- `engine_id` (string, required): Engine to run the query on (from `list_engines`, must be running)

**Returns**:
- `query_id` (string): Unique query identifier
- `rows_updated` (integer): Number of rows updated (if available)
- `execution_time_ms` (integer): Query duration in milliseconds
- `status` (string): Execution status (`"success"`)
- `catalog_name` (string): Echo of input catalog
- `schema_name` (string): Echo of input schema

**Safety Features**:
- ✅ Only UPDATE statements allowed
- ❌ Rejects any query not starting with UPDATE
- ❌ Rejects multiple statements separated by semicolons
- ⏱ 120-second execution timeout

**Example Usage:**

**Natural language:**
```
Update the email for customer 9001 to 'alice.smith@example.com'
```

**Claude executes:**
```sql
UPDATE iceberg_data.sales_db.customers
SET email = 'alice.smith@example.com'
WHERE customer_id = 9001
```

**Claude responds:**
```
✓ UPDATE executed successfully

Rows updated: 1
Query ID: q-20250101-002
Execution time: 280ms
```

**Use Cases:**
- Correct data entry mistakes
- Apply bulk field changes with WHERE filters
- Update status or flag columns
- Synchronise data during migrations

**Important Notes:**
- UPDATE support requires Iceberg tables (not Hive/CSV)
- Always include a WHERE clause to avoid unintended full-table updates
- Check the row count in the response to confirm expected impact

**Best Practices:**
- Run a SELECT with the same WHERE clause first to preview affected rows
- Use fully qualified table names: `catalog.schema.table`
- Test with a narrow WHERE filter before broadening the scope

---

### explain_query

Get query execution plan without running the query to understand optimization and resource usage.

**Category**: Query Analysis

**Parameters**:
- `engine_id` (string, required): Presto or Prestissimo engine identifier
- `statement` (string, required): SQL query to explain
- `engine_type` (string, optional): "presto" or "prestissimo" (default: "presto")
- `format` (string, optional): "json" or "text"
- `type` (string, optional): "logical", "distributed", "validate", or "io"

**Returns**:
- `engine_id` (string): Engine identifier used
- `engine_type` (string): Engine type
- `statement` (string): Query that was explained
- `plan` (string): Query execution plan
- `response` (object): Full API response

**Example Usage:**

**Natural language:**
```
Explain how this query will be executed:
SELECT c.customer_id, COUNT(o.order_id) FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id
```

**Claude responds:**
```
Query Execution Plan:

Fragment 0 [SINGLE]
    Output: [customer_id, count]
    - Aggregate(FINAL)[customer_id]
        - LocalExchange[HASH]
            - RemoteSource[1]

Fragment 1 [HASH]
    - Aggregate(PARTIAL)[customer_id]
        - LeftJoin[customer_id = customer_id]
            - TableScan[customers] (10,000 rows)
            - TableScan[orders] (50,000 rows)

This plan shows hash-based join and two-stage aggregation.
```

**Use Cases:**
- Understand query performance before execution
- Identify optimization opportunities
- Debug slow queries
- Validate query correctness

**Best Practices:**
- Use `type="distributed"` to see data movement
- Check for table scans on large tables
- Look for partition pruning in WHERE clauses
- Use before running expensive queries

---

### explain_analyze_query

Get detailed query analysis with actual execution statistics to measure real performance.

**Category**: Query Analysis

**Parameters**:
- `engine_id` (string, required): Presto or Prestissimo engine identifier
- `statement` (string, required): SQL query to analyze
- `engine_type` (string, optional): "presto" or "prestissimo" (default: "presto")
- `verbose` (boolean, optional): Include detailed statistics

**Returns**:
- `engine_id` (string): Engine identifier used
- `engine_type` (string): Engine type
- `statement` (string): Query that was analyzed
- `analysis` (string): Detailed execution analysis with statistics
- `response` (object): Full API response

**Example Usage:**

**Natural language:**
```
Analyze the performance of:
SELECT region, COUNT(*) as orders, SUM(total) as revenue
FROM orders WHERE order_date >= DATE '2024-01-01'
GROUP BY region
```

**Claude responds:**
```
Query Performance Analysis:

Fragment 0 [SINGLE]
    CPU: 1.23s, Input: 5 rows, Output: 5 rows
    - Aggregate(FINAL)[region]

Fragment 1 [HASH]
    CPU: 5.67s, Input: 1M rows, Output: 5 rows
    - Aggregate(PARTIAL)[region]
        - ScanFilterProject[orders]
            Scanned: 5M rows (625 MB)
            Filtered: 4M rows (80% reduction)

Performance Summary:
- Total CPU: 6.90s
- Execution time: 11.37s
- Data scanned: 625 MB
- Final output: 5 rows

✓ Partition pruning reduced scan by 80%
```

**Use Cases:**
- Measure actual query performance
- Identify performance bottlenecks
- Compare query variations
- Troubleshoot slow queries

**Best Practices:**
- Use `verbose=true` for detailed statistics
- Compare with explain_query to see estimated vs actual
- Look for high CPU time or data scanned
- Use to validate optimization improvements

---

## Spark Application Tools

Tools for submitting and managing Spark applications on watsonx.data Spark engines.

### submit_spark_application

Submit a Spark application for execution on a Spark engine.

**Category**: Spark Application Management

**Parameters**:
- `engine_id` (string, required): Spark engine identifier (from `list_engines`)
- `application` (string, required): Application file path — JAR, Python (.py), or R file
- `arguments` (array of strings, optional): Command-line arguments passed to the application
- `conf` (object, optional): Spark configuration properties (e.g., `{"spark.executor.memory": "2g"}`)
- `env` (object, optional): Environment variables for the application
- `name` (string, optional): Human-readable application name
- `job_endpoint` (string, optional): External job endpoint URL
- `service_instance_id` (string, optional): Service instance ID
- `type` (string, optional): Application type — `"iae"` or `"emr"`
- `volumes` (array of objects, optional): Volume mounts for data access

**Returns**:
- `application_id` (string): Unique application identifier
- `state` (string): Initial application state (e.g., `"accepted"`, `"running"`)
- Additional submission details from the API

**Example Usage:**

**Natural language:**
```
Submit the ETL job at s3://my-bucket/jobs/etl.py on spark-engine-01 with 4 executors
```

**Claude executes:**
```
submit_spark_application(
  engine_id="spark-engine-01",
  application="s3://my-bucket/jobs/etl.py",
  conf={"spark.executor.instances": "4"},
  name="ETL Job"
)
```

**Claude responds:**
```
✓ Spark application submitted

Application ID: app-20250101-abc123
State: accepted
Engine: spark-engine-01

Use get_spark_application_status to monitor progress.
```

**Use Cases:**
- Run ETL/ELT batch processing jobs
- Execute data transformation pipelines
- Run ML training workloads on Spark
- Submit scheduled or on-demand Spark jobs

**Best Practices:**
- Always provide a `name` for easier identification
- Use `conf` to tune executor memory/cores for large workloads
- Store application files in accessible object storage (e.g., S3/COS)
- Poll status with `get_spark_application_status` after submission

---

### list_spark_applications

List Spark applications on a Spark engine, optionally filtered by state.

**Category**: Spark Application Management

**Parameters**:
- `engine_id` (string, required): Spark engine identifier
- `state` (array of strings, optional): Filter by application state
  - Values: `"running"`, `"finished"`, `"failed"`, `"accepted"`, `"unknown"`
  - Default: Returns all states

**Returns**:
- `applications` (array): List of application objects, each containing:
  - `application_id` (string): Application identifier
  - `state` (string): Current state
  - `name` (string): Application name (if set)
  - `start_time` (string): Submission timestamp
  - Additional application metadata

**Example Usage:**

**Natural language:**
```
List all running Spark applications on spark-engine-01
```

**Claude responds:**
```
Running Spark Applications on spark-engine-01:

| Application ID        | Name         | State   | Started             |
|-----------------------|--------------|---------|---------------------|
| app-20250101-abc123   | ETL Job      | running | 2025-01-01 10:05:00 |
| app-20250101-def456   | ML Training  | running | 2025-01-01 10:12:00 |

2 application(s) running.
```

**Use Cases:**
- Monitor active workloads on an engine
- Audit running jobs before pausing an engine
- Identify failed jobs for troubleshooting
- Check job history after batch processing

---

### get_spark_application_status

Get detailed status of a specific Spark application.

**Category**: Spark Application Management

**Parameters**:
- `engine_id` (string, required): Spark engine identifier
- `application_id` (string, required): Application identifier (from `submit_spark_application` or `list_spark_applications`)

**Returns**:
- `application_id` (string): Application identifier
- `state` (string): Current state (e.g., `"running"`, `"finished"`, `"failed"`)
- `start_time` (string): Application start timestamp
- `end_time` (string): End timestamp (if completed)
- `spark_version` (string): Spark version used
- `application_details` (object): Configuration and runtime details

**Example Usage:**

**Natural language:**
```
What is the status of Spark application app-20250101-abc123?
```

**Claude responds:**
```
Spark Application Status:

Application ID: app-20250101-abc123
State: finished
Started:  2025-01-01 10:05:00
Finished: 2025-01-01 10:18:42
Duration: ~14 minutes
Spark version: 3.3.0
```

**Use Cases:**
- Poll for job completion in automated pipelines
- Diagnose failed applications
- Confirm a job completed before downstream processing
- Audit execution details after job completion

---

### stop_spark_application

Stop and remove a running Spark application.

**Category**: Spark Application Management

**Parameters**:
- `engine_id` (string, required): Spark engine identifier
- `application_id` (string, required): Application identifier to stop

**Returns**:
- Operation status from the API

**Example Usage:**

**Natural language:**
```
Stop Spark application app-20250101-abc123 on spark-engine-01
```

**Claude responds:**
```
✓ Spark application stopped

Application ID: app-20250101-abc123
Engine: spark-engine-01

The application has been terminated and removed from the engine's history.
```

**Use Cases:**
- Cancel a long-running or stuck job
- Free up engine resources
- Terminate jobs submitted with incorrect parameters

**Important Notes:**
- Stopping an application is irreversible — in-progress work will be lost
- The application is removed from the engine's history after stopping
- Use `get_spark_application_status` first to confirm the application is still running

---

## Data Ingestion Tools

### create_ingestion_job

Create a data ingestion job to load data from external sources into watsonx.data tables.

**Category**: Data Ingestion

**Parameters**:
- `job_id` (string, required): Unique job identifier (e.g., "ingestion-1234567890")
- `catalog` (string, required): Target catalog name
- `schema` (string, required): Target schema name
- `table` (string, required): Target table name
- `file_paths` (string, required): Source file path (e.g., "s3://bucket-name/file.csv")
- `file_type` (string, optional): Source file type - "csv", "parquet", "json" (default: "csv")
- `bucket_name` (string, optional): S3 bucket name (extracted from file_paths if not provided)
- `bucket_type` (string, optional): Bucket type - "ibm_cos", "aws_s3" (default: "ibm_cos")
- `write_mode` (string, optional): Write mode - "append", "overwrite" (default: "append")
- `engine_id` (string, optional): Spark engine ID to use for ingestion
- `field_delimiter` (string, optional): CSV field delimiter (default: ",")
- `line_delimiter` (string, optional): CSV line delimiter (default: "\n")
- `escape_character` (string, optional): CSV escape character (default: "\\")
- `header` (boolean, optional): Whether CSV has header row (default: true)
- `encoding` (string, optional): File encoding (default: "UTF-8")
- `driver_memory` (string, optional): Spark driver memory (default: "2G")
- `driver_cores` (integer, optional): Spark driver cores (default: 1)
- `executor_memory` (string, optional): Spark executor memory (default: "2G")
- `executor_cores` (integer, optional): Spark executor cores (default: 1)
- `num_executors` (integer, optional): Number of Spark executors (default: 1)

**Returns**:
- `job_id` (string): Job identifier
- `status` (string): Job status ("queued", "running", "completed", "failed")
- `create_time` (string): Job creation timestamp
- Additional job configuration details

**Example Usage:**

**Natural language:**
```
Load the CSV file from s3://my-bucket/sales/2024/sales_data.csv into the iceberg_data catalog, sales_db schema, sales_2024 table
```

**Claude responds:**
```
Creating ingestion job to load CSV data...

Job Created:
- Job ID: ingest-sales-2024-001
- Source: s3://my-bucket/sales/2024/sales_data.csv
- Target: iceberg_data.sales_db.sales_2024
- Bucket: my-bucket (ibm_cos)
- File Type: CSV
- Write Mode: append
- Status: queued

The job has been queued and will start processing shortly.
Use get_ingestion_job to monitor progress.
```

**Parquet file example:**
```
Ingest parquet files from s3://data-lake/events/user_events.parquet into iceberg_data.events.user_events with overwrite mode
```

**Claude executes:**
```
Creating ingestion job for Parquet data...

Job Configuration:
- Job ID: ingest-events-001
- Source: s3://data-lake/events/user_events.parquet
- Target: iceberg_data.events.user_events
- File Type: Parquet
- Write Mode: overwrite
- Status: queued

The data will overwrite existing data in the target table.
```

**Use Cases:**
- Bulk data loading from S3/COS
- ETL pipeline data ingestion
- Historical data migration
- Regular data imports
- Data lake ingestion

**Best Practices:**
- Use descriptive job IDs for tracking
- Specify partition columns for large datasets
- Configure Spark resources based on data size
- Use appropriate file formats (Parquet for analytics)
- Test with small datasets first

---

### list_ingestion_jobs

List all data ingestion jobs with their status and configuration.

**Category**: Data Ingestion

**Parameters**:
- `start` (integer, optional): Offset for pagination (default: 0, 0-based)
- `limit` (integer, optional): Number of jobs per page (default: 10, max: 100, -1 for all)

**Returns**:
- `ingestion_jobs` (array): List of ingestion jobs
  - `job_id` (string): Job identifier
  - `status` (string): Job status
  - `source_data_files` (string): Source path
  - `target_table` (string): Target table
  - `create_time` (string): Creation timestamp
  - `start_time` (string): Start timestamp (if started)
  - `end_time` (string): End timestamp (if completed)
- `total_count` (integer): Total number of jobs
- `page` (integer): Current page number

**Example Usage:**

**Natural language:**
```
Show me all ingestion jobs
```

**Claude responds:**
```
Ingestion Jobs:

Active Jobs (2):
1. ingest-sales-2024-001
   Status: running (45% complete)
   Source: s3://my-bucket/sales/2024/sales_data.csv
   Target: iceberg_data.sales_db.raw.sales_2024
   Started: 5 minutes ago
   Progress: 1.2M rows processed

2. ingest-events-001
   Status: queued
   Source: s3://data-lake/events/*.parquet
   Target: iceberg_data.events.raw.user_events
   Created: 2 minutes ago

Completed Jobs (3):
3. ingest-customers-123
   Status: completed
   Duration: 12 minutes
   Rows: 5.2M rows ingested
   Completed: 1 hour ago

4. ingest-products-456
   Status: completed
   Duration: 3 minutes
   Rows: 50K rows ingested
   Completed: 3 hours ago

5. ingest-orders-789
   Status: failed
   Error: Schema mismatch in column 'order_date'
   Failed: 5 hours ago

Total: 5 jobs
```

**Use Cases:**
- Monitor ingestion pipeline status
- Track job history
- Identify failed jobs
- Audit data loading activities

---

### get_ingestion_job

Get detailed status and progress of a specific ingestion job.

**Category**: Data Ingestion

**Parameters**:
- `job_id` (string, required): Job identifier

**Returns**:
- `job_id` (string): Job identifier
- `status` (string): Current status
- `source_data_files` (string): Source path
- `target_table` (string): Target table
- `username` (string): User who created the job
- `create_time` (string): Creation timestamp
- `start_time` (string): Start timestamp (if started)
- `end_time` (string): End timestamp (if completed)
- `progress` (number): Completion percentage (if running)
- `rows_processed` (integer): Number of rows processed
- `bytes_processed` (integer): Bytes processed
- `error_message` (string): Error details (if failed)

**Example Usage:**

**Natural language:**
```
What's the status of ingestion job ingest-sales-2024-001?
```

**Claude responds:**
```
Ingestion Job Status: ingest-sales-2024-001

Status: running
Progress: 67.5%

Job Details:
- Source: s3://my-bucket/sales/2024/sales_data.csv
- Target: iceberg_data.sales_db.raw.sales_2024
- File Type: CSV
- User: data_engineer

Timing:
- Created: 2024-01-15 10:30:00 UTC
- Started: 2024-01-15 10:31:15 UTC
- Running for: 8 minutes 45 seconds

Progress:
- Rows processed: 3,450,000
- Data processed: 1.2 GB
- Estimated completion: 4 minutes

The job is progressing normally. Data is being written to the target
table and will be available for querying once complete.
```

**Failed job example:**
```
Check status of job ingest-orders-789
```

**Claude responds:**
```
Ingestion Job Status: ingest-orders-789

Status: ❌ FAILED

Job Details:
- Source: s3://my-bucket/orders/orders.csv
- Target: iceberg_data.sales_db.raw.orders
- User: etl_user

Timing:
- Created: 2024-01-15 05:00:00 UTC
- Started: 2024-01-15 05:01:00 UTC
- Failed: 2024-01-15 05:05:30 UTC
- Duration: 4 minutes 30 seconds

Error Details:
Schema mismatch: expected INT but found STRING in column 'order_id'
at row 125,430

Recommendation:
- Verify the source data schema matches the target table
- Check for data quality issues in the source file
- Consider using schema inference or explicit schema definition
```

**Use Cases:**
- Monitor long-running ingestion jobs
- Debug failed ingestions
- Track data loading progress
- Verify job completion

---

### delete_ingestion_job

Cancel a running ingestion job or remove a job from history.

**Category**: Data Ingestion

**Important**: Jobs in 'failed' or 'completed' status cannot be deleted. Only jobs in 'starting', 'running', or 'queued' status can be deleted.

**Parameters**:
- `job_id` (string, required): Job identifier

**Returns**:
- Dict with deletion status

**Example Usage:**

**Natural language:**
```
Cancel ingestion job ingest-sales-2024-001
```

**Claude responds:**
```
Cancelling ingestion job...

Job Cancelled: ingest-sales-2024-001
- Status: cancelled
- Rows processed before cancellation: 1,250,000
- Partial data may have been written to target table

The job has been stopped and removed from the queue.
Note: Partial data that was already written to the target table
will remain. You may need to clean up the target table if needed.
```

**Completed job deletion:**
```
Delete the completed job ingest-customers-123 from history
```

**Claude responds:**
```
Ingestion job deleted successfully.

Job ingest-customers-123 has been removed from the job history.
The ingested data in the target table remains unchanged.
```

**Use Cases:**
- Cancel long-running jobs that are no longer needed
- Clean up job history
- Stop jobs consuming excessive resources
- Remove failed job records

**Best Practices:**
- Verify job status before deletion
- Consider if partial results are needed
- Document reason for cancellation
- Check target table for partial data after cancellation

**Safety Notes:**
- Deletion is immediate and cannot be undone
- Running jobs will be forcefully terminated
- Partial data may remain in target table
- Use with caution in production environments

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
- SELECT queries are read-only and safe for production use
- `execute_insert` and `execute_update` modify data — use with care
- DDL operations (create schema, rename table, add/rename columns) modify schema structure
- No administrative commands
- Spark applications run with the permissions of the configured service credentials

**Credentials:**
- API keys are managed by server
- No need to handle authentication
- Automatic IAM token refresh
- TLS encryption for all API calls

---

