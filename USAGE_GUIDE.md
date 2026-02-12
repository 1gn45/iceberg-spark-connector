# Usage Guide

## Installation

```bash
pip install -e .
```

## Environment Setup

Create a `.env` file in your project directory:

```env
# Catalog type: hadoop, hive, rest, glue
ICEBERG_CATALOG_TYPE=hadoop

# Warehouse path
ICEBERG_WAREHOUSE_PATH=/tmp/iceberg-warehouse

# Spark master
SPARK_MASTER=local[*]

# For Hive catalog
# ICEBERG_CATALOG_URI=thrift://localhost:9083

# For AWS Glue
# AWS_REGION=us-east-1
```

## Quick Start

### Basic Operations

```python
from iceberg_connector import IcebergConnector

# Initialize
conn = IcebergConnector(
    namespace="my_database",
    catalog="iceberg_catalog"
)

# Create a table
conn.spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg_catalog.my_database.products (
        id STRING,
        name STRING,
        price DOUBLE,
        category STRING,
        in_stock BOOLEAN
    ) USING iceberg
""")

# Insert
conn.insert_row_data("products", {
    "id": "p001",
    "name": "Laptop",
    "price": 999.99,
    "category": "Electronics",
    "in_stock": True
})

# Read
product = conn.fetch_row_by_id("products", "p001")
print(product)

# Update
conn.update_row_data("products", "p001", {"price": 899.99})

# Delete
conn.delete_row("products", "p001")
```

### Advanced Filtering

```python
# Complex filters
results = conn.get_table_with_filters(
    table="orders",
    filters={
        "status": "pending",
        "total": (100, 1000),  # Range
        "region": ["US", "EU", "APAC"],  # IN list
        "created_date": {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31"
        }
    },
    order_by="created_date",
    order_direction="DESC",
    limit=50,
    offset=0
)
```

### Batch Operations

```python
import pandas as pd

# Batch insert
new_products = pd.DataFrame({
    "id": ["p002", "p003", "p004"],
    "name": ["Mouse", "Keyboard", "Monitor"],
    "price": [29.99, 79.99, 299.99],
    "category": ["Electronics", "Electronics", "Electronics"],
    "in_stock": [True, True, False]
})

conn.update_dataframe_data("products", df_inserts=new_products)

# Batch update
updates = pd.DataFrame({
    "id": ["p002", "p003"],
    "price": [24.99, 69.99]
})

conn.update_dataframe_data("products", df_updates=updates)
```

## File Operations

### Loading Data from Files

The connector supports loading data from CSV, JSON, and Parquet files:

```python
# Load from CSV
conn.load_data_from_file(
    table="products",
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
    file_path="s3://bucket/data/*.parquet",
    file_format="parquet",
    mode="overwrite"
)
```

### Exporting Data to Files

Export table data with optional filtering:

```python
# Export to CSV
conn.export_table_to_file(
    table="orders",
    file_path="/output/orders.csv",
    file_format="csv",
    options={"header": "true"},
    filters={"status": "completed"}
)

# Export to Parquet (better for large datasets)
conn.export_table_to_file(
    table="transactions",
    file_path="/output/transactions.parquet",
    file_format="parquet",
    filters={"date": {"start_date": "2024-01-01", "end_date": "2024-12-31"}}
)

# Export to JSON
conn.export_table_to_file(
    table="users",
    file_path="/output/users.json",
    file_format="json"
)
```

## Table Management

### Creating Tables from DataFrames

Create Iceberg tables directly from pandas DataFrames:

```python
import pandas as pd

# Create DataFrame
df = pd.DataFrame({
    "customer_id": ["c001", "c002", "c003"],
    "name": ["Alice", "Bob", "Charlie"],
    "country": ["USA", "UK", "Canada"]
})

# Create table
conn.create_table_from_dataframe(
    table="customers",
    df=df,
    partition_by=["country"]  # Optional partitioning
)
```

### Cloning Tables

Create copies of existing tables:

```python
# Clone with data (backup)
conn.clone_table(
    source_table="customers",
    target_table="customers_backup",
    include_data=True
)

# Clone only schema (template)
conn.clone_table(
    source_table="customers",
    target_table="customers_template",
    include_data=False
)
```

### Dropping and Truncating Tables

```python
# Drop table (keep metadata)
conn.drop_table("old_table")

# Drop table and purge all files
conn.drop_table("old_table", purge=True)

# Truncate table (delete data, keep schema)
conn.truncate_table("temp_table")
```

## Schema Evolution

Apache Iceberg supports schema evolution without rewriting data:

### Adding Columns

```python
# Add a new column
conn.add_column(
    table="products",
    column_name="supplier_id",
    column_type="STRING",
    comment="Supplier identifier"
)

# Add multiple data types
conn.add_column(table="orders", column_name="discount", column_type="DOUBLE")
conn.add_column(table="orders", column_name="notes", column_type="STRING")
conn.add_column(table="orders", column_name="processed", column_type="BOOLEAN")
```

### Dropping Columns

```python
# Drop a column
conn.drop_column(table="products", column_name="old_field")
```

### Renaming Columns

```python
# Rename a column
conn.rename_column(
    table="customers",
    old_name="email",
    new_name="email_address"
)
```

## View Operations

Create and manage views for complex queries:

```python
# Create a view
conn.create_view(
    view_name="high_value_customers",
    query="""
        SELECT customer_id, name, SUM(order_total) as total_spent
        FROM iceberg_catalog.my_database.orders
        WHERE status = 'completed'
        GROUP BY customer_id, name
        HAVING SUM(order_total) > 1000
    """,
    replace=False
)

# Create or replace view
conn.create_view(
    view_name="active_users",
    query="SELECT * FROM iceberg_catalog.my_database.users WHERE active = true",
    replace=True
)

# Drop a view
conn.drop_view("old_view")
```

## Connection & Metadata Operations

### Testing Connection

```python
# Test connection and get session info
conn_info = conn.test_connection()
print(f"Status: {conn_info['status']}")
print(f"Catalog: {conn_info['catalog']}")
print(f"Spark Version: {conn_info['spark_version']}")
```

### Listing Tables

```python
# List all tables in current namespace
tables = conn.list_tables()
print(f"Tables: {tables}")

# List tables in specific namespace
tables = conn.list_tables(namespace="other_database")
```

### Getting Table Statistics

```python
# Get comprehensive table stats
stats = conn.get_table_stats("products")
print(f"Rows: {stats['row_count']}")
print(f"Columns: {stats['column_count']}")
print(f"Schema: {stats['columns']}")
```

### Time Travel (Iceberg Feature)

```python
# Get snapshot history
history = conn.get_table_history("products")
print(history)

# Query old snapshot
snapshot_id = 1234567890
old_data = conn.read_table_at_snapshot("products", snapshot_id)
print(old_data)
```

### Schema Operations

```python
# Get table schema
schema = conn.get_table_schema("products")
for col_name, col_type in schema:
    print(f"{col_name}: {col_type}")

# Check if table exists
if conn.table_exists("products"):
    print("Table exists!")

# Create namespace
conn.create_namespace("analytics")
```

### Raw SQL

```python
# Execute custom queries
result = conn.run_sql("""
    SELECT category, COUNT(*) as count, AVG(price) as avg_price
    FROM iceberg_catalog.my_database.products
    WHERE in_stock = true
    GROUP BY category
""")
print(result)
```

## Catalog Configurations

### Local Development (Hadoop Catalog)

```python
from iceberg_connector import get_spark_session

spark = get_spark_session(
    catalog_type="hadoop",
    warehouse_path="/tmp/iceberg-warehouse"
)

conn = IcebergConnector(
    namespace="my_db",
    catalog="iceberg_catalog",
    spark=spark
)
```

### Hive Metastore

```python
spark = get_spark_session(
    catalog_type="hive",
    catalog_uri="thrift://localhost:9083",
    warehouse_path="/user/hive/warehouse"
)
```

### AWS Glue

```python
spark = get_spark_session(
    catalog_type="glue",
    warehouse_path="s3://my-bucket/warehouse",
    aws_region="us-east-1"
)
```

### REST Catalog

```python
spark = get_spark_session(
    catalog_type="rest",
    catalog_uri="http://localhost:8181"
)
```

## Best Practices

### 1. Use Partitioning for Large Tables

```sql
CREATE TABLE iceberg_catalog.my_database.events (
    id STRING,
    event_type STRING,
    user_id STRING,
    timestamp TIMESTAMP,
    data STRING
) USING iceberg
PARTITIONED BY (days(timestamp))
```

### 2. Batch Operations Over Single Inserts

Use `update_dataframe_data()` for bulk operations instead of looping `insert_row_data()`.

### 3. Regular Maintenance

```python
# Expire old snapshots
conn.spark.sql("""
    CALL iceberg_catalog.system.expire_snapshots(
        table => 'my_database.products',
        older_than => TIMESTAMP '2024-01-01 00:00:00'
    )
""")

# Remove orphan files
conn.spark.sql("""
    CALL iceberg_catalog.system.remove_orphan_files(
        table => 'my_database.products'
    )
""")
```

### 4. Error Handling

```python
try:
    conn.insert_row_data("products", {...})
except ValueError as e:
    print(f"Error: {e}")
```

## Troubleshooting

### Issue: Table not found

**Solution:** Ensure the namespace and catalog are correct:

```python
conn.spark.sql("SHOW NAMESPACES IN iceberg_catalog").show()
conn.spark.sql("SHOW TABLES IN iceberg_catalog.my_database").show()
```

### Issue: Permission errors

**Solution:** Check warehouse path permissions and ensure the Spark user has write access.

### Issue: Slow queries

**Solution:**
- Add partitioning to large tables
- Use filters to leverage partition pruning
- Run table compaction:

```python
conn.spark.sql("""
    CALL iceberg_catalog.system.rewrite_data_files(
        table => 'my_database.large_table'
    )
""")
```

## Additional Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [GitHub Issues](https://github.com/1gn45/iceberg-spark-connector/issues)
