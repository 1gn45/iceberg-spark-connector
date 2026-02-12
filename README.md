# Apache Iceberg Spark Connector

A clean, minimal Apache Iceberg database connector library built on top of PySpark. Provides simple CRUD operations and table management for Iceberg tables.

## Features

- 🔐 **Session Management**: Automatic SparkSession handling for Iceberg catalogs
- 📊 **CRUD Operations**: Simple methods for Create, Read, Update, Delete operations
- 🔍 **Flexible Filtering**: Advanced filtering, sorting, and pagination
- 🔄 **Transaction Support**: ACID operations with Iceberg's native transaction support
- 🏠 **Local Development**: Support for local Hadoop/Iceberg catalog
- 🗄️ **Multiple Catalogs**: Support for various catalog backends (Hive, Hadoop, REST, Glue, etc.)
- 📁 **File Operations**: Load and export data from/to CSV, JSON, and Parquet files
- 🔧 **Schema Evolution**: Add, drop, and rename columns without data rewriting
- 📋 **Table Management**: Clone, drop, truncate tables with ease
- 👁️ **View Support**: Create and manage views for complex queries
- 📊 **Bulk Loading**: Efficient batch operations for large datasets
- 🔍 **Metadata Operations**: Get table stats, list tables, test connections

## Installation

### From Source

```bash
git clone https://github.com/1gn45/iceberg-spark-connector.git
cd iceberg-spark-connector
pip install -e .
```

### From GitHub

```bash
pip install git+https://github.com/1gn45/iceberg-spark-connector.git
```

## Quick Start

### 1. Set Up Environment Variables

Create a `.env` file:

```env
ICEBERG_CATALOG_TYPE=hadoop  # or hive, rest, glue
ICEBERG_WAREHOUSE_PATH=/path/to/warehouse
ICEBERG_CATALOG_URI=thrift://localhost:9083  # for Hive metastore
SPARK_MASTER=local[*]  # or your Spark master URL
```

### 2. Basic Usage

```python
from iceberg_connector import IcebergConnector, get_spark_session

# Option 1: Automatic session (uses environment variables)
conn = IcebergConnector(
    namespace="my_database",
    catalog="my_catalog"
)

# Option 2: Manual session creation
spark = get_spark_session()
conn = IcebergConnector(
    namespace="my_database",
    catalog="my_catalog",
    spark=spark
)
```

### 3. CRUD Operations

#### Read Operations

```python
# Fetch a single row by ID
row = conn.fetch_row_by_id(table="users", row_id="123")

# Fetch rows by field value
users = conn.fetch_row_by_field_value(
    table="users", 
    field="status", 
    value="active"
)

# Get table with filters
df = conn.get_table_with_filters(
    table="orders",
    filters={
        "status": "pending",
        "created_date": {"start_date": "2024-01-01", "end_date": "2024-12-31"},
        "amount": (100, 1000),  # Range
        "region": ["US", "EU"]  # IN list
    },
    order_by="created_date",
    order_direction="DESC",
    limit=100,
    offset=0
)

# Get count of filtered rows
count = conn.get_filtered_rows_count(
    table="orders",
    filters={"status": "pending"}
)

# Get unique column values
regions = conn.get_unique_column_values(
    table="orders",
    column_name="region"
)
```

#### Write Operations

```python
# Insert a new row
conn.insert_row_data(
    table="users",
    row_data={
        "id": "new_user_123",
        "name": "John Doe",
        "email": "john@example.com",
        "status": "active"
    }
)

# Update an existing row (using merge/upsert)
conn.update_row_data(
    table="users",
    row_id="new_user_123",
    updated_values={
        "name": "Jane Doe",
        "email": "jane@example.com"
    }
)

# Batch update from DataFrames
import pandas as pd

updates_df = pd.DataFrame({
    "id": ["user1", "user2"],
    "name": ["Updated Name 1", "Updated Name 2"],
    "status": ["active", "inactive"]
})

inserts_df = pd.DataFrame({
    "id": ["user3"],
    "name": ["New User"],
    "status": ["active"]
})

conn.update_dataframe_data(
    table="users",
    df_updates=updates_df,
    df_inserts=inserts_df
)

# Delete a row
conn.delete_row(table="users", row_id="user_to_delete")

# Upsert data (insert or update in one operation)
upsert_df = pd.DataFrame({
    "id": ["user1", "user2", "user3"],
    "name": ["Updated User 1", "Updated User 2", "New User 3"],
    "status": ["active", "active", "pending"]
})

conn.upsert_dataframe_data(
    table="users",
    df=upsert_df,
    id_column="id"
)
```

#### Utility Operations

```python
# Execute raw SQL
result = conn.run_sql("SELECT COUNT(*) FROM my_catalog.my_database.users")

# Check if table exists
exists = conn.table_exists("users")

# Create namespace (database)
conn.create_namespace("new_database")

# Remove duplicate rows
status = conn.remove_duplicate_rows(
    table="users",
    columns=["email"]  # Check for duplicates based on email
)

# Get table schema
schema = conn.get_table_schema("users")

# View table history (Iceberg snapshots)
history = conn.get_table_history("users")

# Time travel query
old_data = conn.read_table_at_snapshot(table="users", snapshot_id=1234567890)

# Test connection
conn_info = conn.test_connection()

# List all tables in namespace
tables = conn.list_tables()

# Get table statistics
stats = conn.get_table_stats("users")

# Get fully qualified table name
full_table_name = conn.get_table_name("users")
# Returns: "iceberg_catalog.my_database.users"
```

#### File Operations

```python
# Load data from CSV file
conn.load_data_from_file(
    table="users",
    file_path="/path/to/data.csv",
    file_format="csv",
    mode="append",
    options={"header": "true", "delimiter": ","}
)

# Load from JSON
conn.load_data_from_file(
    table="events",
    file_path="/path/to/events.json",
    file_format="json",
    mode="append"
)

# Load from Parquet
conn.load_data_from_file(
    table="analytics",
    file_path="/path/to/data.parquet",
    file_format="parquet",
    mode="overwrite"
)

# Export table to CSV
conn.export_table_to_file(
    table="users",
    file_path="/output/users.csv",
    file_format="csv",
    options={"header": "true"},
    filters={"status": "active"}  # Optional filtering
)

# Export to Parquet
conn.export_table_to_file(
    table="orders",
    file_path="/output/orders.parquet",
    file_format="parquet"
)
```

#### Table Management

```python
# Create table from pandas DataFrame
import pandas as pd

df = pd.DataFrame({
    "id": ["1", "2", "3"],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35]
})

conn.create_table_from_dataframe(
    table="new_users",
    df=df,
    partition_by=["age"]  # Optional partitioning
)

# Clone a table (with data)
conn.clone_table(
    source_table="users",
    target_table="users_backup",
    include_data=True
)

# Clone only schema (without data)
conn.clone_table(
    source_table="users",
    target_table="users_template",
    include_data=False
)

# Drop table
conn.drop_table("old_table", purge=True)

# Truncate table (keep schema, delete data)
conn.truncate_table("temp_table")
```

#### Schema Operations

```python
# Add a new column
conn.add_column(
    table="users",
    column_name="phone",
    column_type="STRING",
    comment="User phone number"
)

# Drop a column
conn.drop_column(table="users", column_name="old_field")

# Rename a column
conn.rename_column(
    table="users",
    old_name="email",
    new_name="email_address"
)
```

#### View Operations

```python
# Create a view
conn.create_view(
    view_name="active_users_view",
    query="SELECT * FROM iceberg_catalog.my_database.users WHERE status = 'active'",
    replace=False
)

# Create or replace view
conn.create_view(
    view_name="user_summary",
    query="SELECT status, COUNT(*) as count FROM iceberg_catalog.my_database.users GROUP BY status",
    replace=True
)

# Drop a view
conn.drop_view("old_view")
```

## Advanced Usage

### Filter Syntax

The `filters` parameter supports multiple value types:

```python
filters = {
    # Exact match (string/int)
    "status": "active",
    
    # Boolean
    "is_verified": True,
    
    # Date range
    "created_date": {
        "start_date": "2024-01-01",
        "end_date": "2024-12-31"
    },
    
    # Numeric range (tuple)
    "amount": (100, 1000),
    
    # IN list
    "category": ["A", "B", "C"],
}
```

### Catalog Configuration

#### Hadoop Catalog (Local)

```python
from iceberg_connector import get_spark_session

spark = get_spark_session(
    catalog_type="hadoop",
    warehouse_path="/tmp/iceberg-warehouse"
)
```

#### Hive Metastore

```python
spark = get_spark_session(
    catalog_type="hive",
    catalog_uri="thrift://localhost:9083",
    warehouse_path="/user/hive/warehouse"
)
```

#### AWS Glue

```python
spark = get_spark_session(
    catalog_type="glue",
    warehouse_path="s3://my-bucket/warehouse",
    aws_region="us-east-1"
)
```

## Architecture

### Core Components

1. **Session Manager** (`session.py`)
   - Handles SparkSession creation
   - Configures Iceberg catalog
   - Support for multiple catalog backends

2. **Main Connector** (`connector.py`)
   - Core CRUD operations using Spark SQL and DataFrame API
   - Filtering, sorting, pagination
   - Iceberg MERGE operations for updates
   - Snapshot and time-travel support

## Requirements

- Python >= 3.8
- pyspark >= 3.3.0
- apache-iceberg >= 1.0.0
- pandas >= 1.3.0
- python-dotenv >= 0.19.0

## Iceberg Features

This connector leverages Apache Iceberg's advanced features:

- **ACID Transactions**: All operations are atomic and isolated
- **Schema Evolution**: Add, drop, rename columns without rewriting data
- **Time Travel**: Query historical data at any snapshot
- **Hidden Partitioning**: Automatic partition management
- **Table Maintenance**: Built-in compaction and cleanup

## License

MIT

## Migration from Snowflake

Looking to replace Snowflake with this connector? Check out the [Snowflake Migration Guide](SNOWFLAKE_MIGRATION.md) for:
- Feature comparison and mapping
- Migration examples
- Advantages of Iceberg over Snowflake
- Deployment options

## Contributing

Contributions welcome! Please feel free to submit a Pull Request.

## Support

For issues and feature requests, please use the [GitHub Issues](https://github.com/1gn45/iceberg-spark-connector/issues) page.
