# Apache Iceberg Spark Connector

A clean, minimal Apache Iceberg database connector library built on top of PySpark. Provides simple CRUD operations and table management for Iceberg tables.

## Features

- 🔐 **Session Management**: Automatic SparkSession handling for Iceberg catalogs
- 📊 **CRUD Operations**: Simple methods for Create, Read, Update, Delete operations
- 🔍 **Flexible Filtering**: Advanced filtering, sorting, and pagination
- 🔄 **Transaction Support**: ACID operations with Iceberg's native transaction support
- 🏠 **Local Development**: Support for local Hadoop/Iceberg catalog
- 🗄️ **Multiple Catalogs**: Support for various catalog backends (Hive, Hadoop, REST, Glue, etc.)

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

## Contributing

Contributions welcome! Please feel free to submit a Pull Request.

## Support

For issues and feature requests, please use the [GitHub Issues](https://github.com/1gn45/iceberg-spark-connector/issues) page.
