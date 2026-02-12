# Snowflake to Iceberg-Spark Migration Guide

## Overview

This document explains how the iceberg-spark-connector can replace Snowflake database connector in data fetching applications, providing a comprehensive feature mapping and migration guide.

## Feature Comparison

### Data Loading

| Snowflake Feature | Iceberg-Spark Equivalent | Notes |
|-------------------|-------------------------|--------|
| `COPY INTO` from files | `load_data_from_file()` | Supports CSV, JSON, Parquet |
| `PUT` file upload | Direct file path access | Use local or remote paths (S3, HDFS, etc.) |
| Stage loading | `load_data_from_file()` with S3/HDFS paths | Cloud storage integration |
| Bulk insert | `upsert_dataframe_data()` or `update_dataframe_data()` | Efficient batch operations |

### Data Manipulation

| Snowflake Feature | Iceberg-Spark Equivalent | Notes |
|-------------------|-------------------------|--------|
| INSERT | `insert_row_data()` | Single row insertion |
| UPDATE | `update_row_data()` | Update by ID |
| DELETE | `delete_row()` | Delete by ID |
| MERGE (UPSERT) | `upsert_dataframe_data()` | Insert or update in one operation |
| Batch INSERT | `update_dataframe_data(df_inserts=df)` | Efficient bulk inserts |
| Batch UPDATE | `update_dataframe_data(df_updates=df)` | Efficient bulk updates |

### Data Retrieval

| Snowflake Feature | Iceberg-Spark Equivalent | Notes |
|-------------------|-------------------------|--------|
| SELECT * | `get_table_with_filters()` | Query with filters, pagination |
| WHERE clause | `filters` parameter | Supports exact match, ranges, IN lists |
| ORDER BY | `order_by`, `order_direction` | ASC/DESC ordering |
| LIMIT/OFFSET | `limit`, `offset` | Pagination support |
| COUNT(*) | `get_filtered_rows_count()` | Count with filters |
| DISTINCT | `get_unique_column_values()` | Get unique values |
| Single row lookup | `fetch_row_by_id()` | By ID or custom field |

### Schema Management

| Snowflake Feature | Iceberg-Spark Equivalent | Notes |
|-------------------|-------------------------|--------|
| CREATE TABLE | `create_table_from_dataframe()` or `create_table_with_sql()` | Multiple creation methods |
| ALTER TABLE ADD COLUMN | `add_column()` | Add columns without rewrite |
| ALTER TABLE DROP COLUMN | `drop_column()` | Drop columns without rewrite |
| ALTER TABLE RENAME COLUMN | `rename_column()` | Rename columns without rewrite |
| DROP TABLE | `drop_table()` | Optional PURGE for file deletion |
| TRUNCATE TABLE | `truncate_table()` | Remove all data, keep schema |
| DESCRIBE TABLE | `get_table_schema()` | Get column names and types |
| SHOW TABLES | `list_tables()` | List all tables in namespace |

### Views

| Snowflake Feature | Iceberg-Spark Equivalent | Notes |
|-------------------|-------------------------|--------|
| CREATE VIEW | `create_view()` | Standard SQL views |
| CREATE OR REPLACE VIEW | `create_view(replace=True)` | Overwrite existing view |
| DROP VIEW | `drop_view()` | Remove view |

### Data Export

| Snowflake Feature | Iceberg-Spark Equivalent | Notes |
|-------------------|-------------------------|--------|
| COPY INTO @stage | `export_table_to_file()` | Export to CSV, JSON, Parquet |
| GET file download | Direct file system access | Write to local or cloud storage |
| Unload to S3 | `export_table_to_file()` with S3 path | Direct S3 integration |

### Transaction Management

| Snowflake Feature | Iceberg-Spark Equivalent | Notes |
|-------------------|-------------------------|--------|
| BEGIN/COMMIT | Automatic ACID transactions | Iceberg native transactions |
| ROLLBACK | Iceberg snapshot management | Time-travel to previous state |
| Isolation levels | Snapshot isolation | Built into Iceberg |

### Time Travel

| Snowflake Feature | Iceberg-Spark Equivalent | Notes |
|-------------------|-------------------------|--------|
| AT(TIMESTAMP => ...) | `read_table_at_snapshot()` | Query historical data |
| BEFORE(STATEMENT => ...) | `get_table_history()` | View snapshot history |
| Time-travel queries | Full Iceberg time-travel support | More granular than Snowflake |

### Partitioning

| Snowflake Feature | Iceberg-Spark Equivalent | Notes |
|-------------------|-------------------------|--------|
| CLUSTER BY | `partition_by` in table creation | Hidden partitioning |
| Automatic clustering | Iceberg hidden partitioning | No manual maintenance |
| Partition pruning | Automatic | Query optimization built-in |

### Connection & Metadata

| Snowflake Feature | Iceberg-Spark Equivalent | Notes |
|-------------------|-------------------------|--------|
| Connection validation | `test_connection()` | Check connection status |
| Database info | Session properties | Catalog, namespace, Spark version |
| Table statistics | `get_table_stats()` | Row count, column count, schema |

## Migration Examples

### Example 1: Simple Data Loading

**Snowflake:**
```python
from snowflake.connector import connect

conn = connect(
    user='user',
    password='password',
    account='account',
    warehouse='warehouse',
    database='database'
)

cursor = conn.cursor()
cursor.execute("COPY INTO table FROM @stage/file.csv")
```

**Iceberg-Spark:**
```python
from iceberg_connector import IcebergConnector

conn = IcebergConnector(
    namespace="database",
    catalog="iceberg_catalog"
)

conn.load_data_from_file(
    table="table",
    file_path="/path/to/file.csv",
    file_format="csv"
)
```

### Example 2: UPSERT Operation

**Snowflake:**
```python
cursor.execute("""
    MERGE INTO target t
    USING source s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET t.value = s.value
    WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)
""")
```

**Iceberg-Spark:**
```python
import pandas as pd

df = pd.DataFrame({
    "id": ["1", "2", "3"],
    "value": ["A", "B", "C"]
})

conn.upsert_dataframe_data(
    table="target",
    df=df,
    id_column="id"
)
```

### Example 3: Filtered Query with Pagination

**Snowflake:**
```python
cursor.execute("""
    SELECT * FROM orders
    WHERE status = 'pending'
    AND amount BETWEEN 100 AND 1000
    ORDER BY created_date DESC
    LIMIT 50 OFFSET 100
""")
results = cursor.fetchall()
```

**Iceberg-Spark:**
```python
results = conn.get_table_with_filters(
    table="orders",
    filters={
        "status": "pending",
        "amount": (100, 1000)
    },
    order_by="created_date",
    order_direction="DESC",
    limit=50,
    offset=100
)
```

### Example 4: Schema Evolution

**Snowflake:**
```python
cursor.execute("ALTER TABLE users ADD COLUMN phone VARCHAR")
cursor.execute("ALTER TABLE users RENAME COLUMN email TO email_address")
cursor.execute("ALTER TABLE users DROP COLUMN old_field")
```

**Iceberg-Spark:**
```python
conn.add_column("users", "phone", "STRING")
conn.rename_column("users", "email", "email_address")
conn.drop_column("users", "old_field")
```

## Advantages of Iceberg-Spark Over Snowflake

### 1. Open Source
- No vendor lock-in
- Community-driven development
- Free to use

### 2. Open Table Format
- Interoperable with multiple engines (Spark, Flink, Presto, Trino)
- Portable data format
- Standard metadata structure

### 3. Cost-Effective
- No compute or storage credits
- Use your own infrastructure
- Pay only for actual resources used

### 4. Advanced Time Travel
- More granular snapshot management
- Efficient storage of historical data
- Fast time-travel queries

### 5. Hidden Partitioning
- No manual partition maintenance
- Automatic partition evolution
- User-transparent optimization

### 6. Local Development
- Full-featured local testing
- No cloud account required for development
- Fast iteration cycles

## Considerations

### Snowflake Features Not Directly Mapped

1. **Data Sharing**: Snowflake's native data sharing requires external solutions
2. **Automatic Query Optimization**: Requires Spark tuning knowledge
3. **Zero-Copy Cloning**: Iceberg cloning involves data copying
4. **Snowpipe**: Need to implement streaming ingestion separately
5. **Resource Management**: Manual Spark cluster management

### Performance Tuning

For optimal performance with Iceberg-Spark:

1. **Partitioning**: Use appropriate partition strategies
```python
conn.create_table_from_dataframe(
    table="events",
    df=df,
    partition_by=["year", "month"]
)
```

2. **File Compaction**: Periodically compact small files
```python
conn.run_sql("""
    CALL iceberg_catalog.system.rewrite_data_files(
        table => 'database.large_table'
    )
""")
```

3. **Snapshot Expiration**: Clean up old snapshots
```python
conn.run_sql("""
    CALL iceberg_catalog.system.expire_snapshots(
        table => 'database.table',
        older_than => TIMESTAMP '2024-01-01 00:00:00'
    )
""")
```

## Deployment Options

### 1. Local Development
```python
conn = IcebergConnector(
    namespace="dev_db",
    catalog="iceberg_catalog"
)
# Uses local Hadoop catalog by default
```

### 2. Hive Metastore
```python
from iceberg_connector import get_spark_session

spark = get_spark_session(
    catalog_type="hive",
    catalog_uri="thrift://metastore:9083"
)
conn = IcebergConnector(namespace="prod_db", spark=spark)
```

### 3. AWS Glue
```python
spark = get_spark_session(
    catalog_type="glue",
    warehouse_path="s3://bucket/warehouse",
    aws_region="us-east-1"
)
conn = IcebergConnector(namespace="prod_db", spark=spark)
```

### 4. REST Catalog
```python
spark = get_spark_session(
    catalog_type="rest",
    catalog_uri="http://catalog-rest:8181"
)
conn = IcebergConnector(namespace="prod_db", spark=spark)
```

## Conclusion

The iceberg-spark-connector provides a comprehensive replacement for Snowflake database connectors in data fetching applications. It offers:

- **Feature Parity**: All essential Snowflake operations are supported
- **Enhanced Capabilities**: Advanced time-travel, schema evolution, and open format
- **Cost Savings**: No licensing fees or compute credits
- **Flexibility**: Multiple deployment options and catalog backends
- **Modern Architecture**: Built on open standards (Apache Iceberg, Apache Spark)

The connector is production-ready and suitable for replacing Snowflake in:
- ETL/ELT pipelines
- Data fetching applications
- Analytics workflows
- Data warehousing solutions
- Open data platforms

For any missing features or enhancements, contributions are welcome via GitHub Issues and Pull Requests.
