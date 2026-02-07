"""
Basic usage example for Iceberg Spark Connector
"""

from iceberg_connector import IcebergConnector
import pandas as pd

# Initialize connector (uses local Hadoop catalog by default)
conn = IcebergConnector(
    namespace="my_database",
    catalog="iceberg_catalog"
)

# Example 1: Create a simple table
print("Creating example table...")
conn.spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg_catalog.my_database.users (
        id STRING,
        name STRING,
        email STRING,
        status STRING,
        created_at TIMESTAMP
    ) USING iceberg
""")

# Example 2: Insert data
print("\nInserting rows...")
conn.insert_row_data(
    table="users",
    row_data={
        "id": "user_001",
        "name": "Alice Smith",
        "email": "alice@example.com",
        "status": "active",
        "created_at": "2024-01-01 10:00:00"
    }
)

conn.insert_row_data(
    table="users",
    row_data={
        "id": "user_002",
        "name": "Bob Jones",
        "email": "bob@example.com",
        "status": "active",
        "created_at": "2024-01-02 11:00:00"
    }
)

# Example 3: Read data
print("\nFetching all users with status='active'...")
active_users = conn.fetch_row_by_field_value(
    table="users",
    field="status",
    value="active"
)
print(active_users)

# Example 4: Fetch single row by ID
print("\nFetching user by ID...")
user = conn.fetch_row_by_id(table="users", row_id="user_001")
print(user)

# Example 5: Update a row
print("\nUpdating user...")
conn.update_row_data(
    table="users",
    row_id="user_001",
    updated_values={
        "name": "Alice Johnson",
        "email": "alice.j@example.com"
    }
)

# Example 6: Get table with filters
print("\nGetting filtered data...")
filtered = conn.get_table_with_filters(
    table="users",
    filters={
        "status": "active"
    },
    order_by="created_at",
    order_direction="DESC",
    limit=10
)
print(filtered)

# Example 7: Batch update from DataFrame
print("\nBatch update from DataFrame...")
updates_df = pd.DataFrame({
    "id": ["user_001", "user_002"],
    "status": ["inactive", "premium"]
})

conn.update_dataframe_data(
    table="users",
    df_updates=updates_df
)

# Example 8: Get count
print("\nCounting active users...")
count = conn.get_filtered_rows_count(
    table="users",
    filters={"status": "active"}
)
print(f"Active users: {count}")

# Example 9: Get table history (Iceberg feature)
print("\nTable snapshot history...")
history = conn.get_table_history("users")
print(history)

# Example 10: Delete a row
print("\nDeleting user...")
conn.delete_row(table="users", row_id="user_002")

print("\n✅ All examples completed!")
