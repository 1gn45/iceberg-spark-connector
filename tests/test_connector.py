"""
Basic tests for IcebergConnector
"""

import pytest
import pandas as pd
from iceberg_connector import IcebergConnector


@pytest.fixture
def connector():
    """Create a test connector with local Hadoop catalog."""
    conn = IcebergConnector(
        namespace="test_db",
        catalog="iceberg_catalog"
    )
    
    # Create test table
    conn.spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg_catalog.test_db.test_users (
            id STRING,
            name STRING,
            email STRING,
            age INT,
            active BOOLEAN
        ) USING iceberg
    """)
    
    yield conn
    
    # Cleanup
    conn.spark.sql("DROP TABLE IF EXISTS iceberg_catalog.test_db.test_users")


def test_insert_row(connector):
    """Test single row insertion."""
    result = connector.insert_row_data(
        table="test_users",
        row_data={
            "id": "test_001",
            "name": "Test User",
            "email": "test@example.com",
            "age": 30,
            "active": True
        }
    )
    assert result is True


def test_fetch_row_by_id(connector):
    """Test fetching a row by ID."""
    # Insert test data
    connector.insert_row_data(
        table="test_users",
        row_data={
            "id": "test_002",
            "name": "Jane Doe",
            "email": "jane@example.com",
            "age": 25,
            "active": True
        }
    )
    
    # Fetch
    row = connector.fetch_row_by_id(table="test_users", row_id="test_002")
    assert row["id"] == "test_002"
    assert row["name"] == "Jane Doe"


def test_update_row(connector):
    """Test row update."""
    # Insert
    connector.insert_row_data(
        table="test_users",
        row_data={
            "id": "test_003",
            "name": "John Smith",
            "email": "john@example.com",
            "age": 35,
            "active": True
        }
    )
    
    # Update
    connector.update_row_data(
        table="test_users",
        row_id="test_003",
        updated_values={"name": "John Doe", "age": 36}
    )
    
    # Verify
    updated_row = connector.fetch_row_by_id(table="test_users", row_id="test_003")
    assert updated_row["name"] == "John Doe"
    assert updated_row["age"] == 36


def test_delete_row(connector):
    """Test row deletion."""
    # Insert
    connector.insert_row_data(
        table="test_users",
        row_data={
            "id": "test_004",
            "name": "Delete Me",
            "email": "delete@example.com",
            "age": 40,
            "active": False
        }
    )
    
    # Delete
    result = connector.delete_row(table="test_users", row_id="test_004")
    assert result is True
    
    # Verify deletion
    with pytest.raises(ValueError):
        connector.fetch_row_by_id(table="test_users", row_id="test_004")


def test_get_table_with_filters(connector):
    """Test filtered query."""
    # Insert test data
    test_data = [
        {"id": "u1", "name": "Alice", "email": "alice@test.com", "age": 25, "active": True},
        {"id": "u2", "name": "Bob", "email": "bob@test.com", "age": 30, "active": True},
        {"id": "u3", "name": "Charlie", "email": "charlie@test.com", "age": 35, "active": False},
    ]
    
    for data in test_data:
        connector.insert_row_data("test_users", data)
    
    # Filter by active status
    results = connector.get_table_with_filters(
        table="test_users",
        filters={"active": True}
    )
    
    assert len(results) == 2
    assert all(results["active"])


def test_batch_operations(connector):
    """Test batch insert and update."""
    # Batch insert
    inserts = pd.DataFrame({
        "id": ["b1", "b2", "b3"],
        "name": ["Batch 1", "Batch 2", "Batch 3"],
        "email": ["b1@test.com", "b2@test.com", "b3@test.com"],
        "age": [20, 21, 22],
        "active": [True, True, True]
    })
    
    result = connector.update_dataframe_data(
        table="test_users",
        df_inserts=inserts
    )
    assert result is True
    
    # Batch update
    updates = pd.DataFrame({
        "id": ["b1", "b2"],
        "age": [25, 26]
    })
    
    result = connector.update_dataframe_data(
        table="test_users",
        df_updates=updates
    )
    assert result is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


def test_load_data_from_csv(connector, tmp_path):
    """Test loading data from CSV file."""
    # Create a test CSV file
    csv_file = tmp_path / "test_data.csv"
    csv_content = """id,name,email,age,active
csv1,CSV User 1,csv1@test.com,25,true
csv2,CSV User 2,csv2@test.com,30,true
"""
    csv_file.write_text(csv_content)
    
    # Load data from CSV
    result = connector.load_data_from_file(
        table="test_users",
        file_path=str(csv_file),
        file_format="csv",
        mode="append"
    )
    assert result is True
    
    # Verify data was loaded
    users = connector.get_table_with_filters("test_users")
    assert len(users) >= 2


def test_export_table_to_file(connector, tmp_path):
    """Test exporting table to file."""
    # Insert test data
    connector.insert_row_data(
        "test_users",
        {"id": "exp1", "name": "Export Test", "email": "exp@test.com", "age": 40, "active": True}
    )
    
    # Export to CSV
    output_file = tmp_path / "output.csv"
    result = connector.export_table_to_file(
        table="test_users",
        file_path=str(output_file),
        file_format="csv"
    )
    assert result is True


def test_create_table_from_dataframe(connector):
    """Test creating table from pandas DataFrame."""
    import pandas as pd
    
    df = pd.DataFrame({
        "id": ["df1", "df2"],
        "value": [100, 200],
        "category": ["A", "B"]
    })
    
    result = connector.create_table_from_dataframe(
        table="test_dataframe_table",
        df=df
    )
    assert result is True
    
    # Verify table was created
    assert connector.table_exists("test_dataframe_table")
    
    # Cleanup
    connector.drop_table("test_dataframe_table")


def test_clone_table(connector):
    """Test table cloning."""
    # Insert test data
    connector.insert_row_data(
        "test_users",
        {"id": "clone1", "name": "Clone Test", "email": "clone@test.com", "age": 35, "active": True}
    )
    
    # Clone with data
    result = connector.clone_table(
        source_table="test_users",
        target_table="test_users_clone",
        include_data=True
    )
    assert result is True
    assert connector.table_exists("test_users_clone")
    
    # Verify data was cloned
    cloned_data = connector.get_table_with_filters("test_users_clone")
    assert len(cloned_data) > 0
    
    # Cleanup
    connector.drop_table("test_users_clone")


def test_add_drop_column(connector):
    """Test adding and dropping columns."""
    # Add column
    result = connector.add_column(
        table="test_users",
        column_name="new_field",
        column_type="STRING",
        comment="Test field"
    )
    assert result is True
    
    # Verify column was added
    schema = connector.get_table_schema("test_users")
    column_names = [col[0] for col in schema]
    assert "new_field" in column_names
    
    # Drop column
    result = connector.drop_column(table="test_users", column_name="new_field")
    assert result is True
    
    # Verify column was dropped
    schema = connector.get_table_schema("test_users")
    column_names = [col[0] for col in schema]
    assert "new_field" not in column_names


def test_rename_column(connector):
    """Test renaming a column."""
    # Add a test column first
    connector.add_column(
        table="test_users",
        column_name="temp_col",
        column_type="STRING"
    )
    
    # Rename column
    result = connector.rename_column(
        table="test_users",
        old_name="temp_col",
        new_name="renamed_col"
    )
    assert result is True
    
    # Verify column was renamed
    schema = connector.get_table_schema("test_users")
    column_names = [col[0] for col in schema]
    assert "renamed_col" in column_names
    assert "temp_col" not in column_names
    
    # Cleanup
    connector.drop_column(table="test_users", column_name="renamed_col")


def test_create_drop_view(connector):
    """Test creating and dropping views."""
    # Insert test data
    connector.insert_row_data(
        "test_users",
        {"id": "view1", "name": "View Test", "email": "view@test.com", "age": 28, "active": True}
    )
    
    # Create view
    full_table = connector.get_table_name("test_users")
    result = connector.create_view(
        view_name="test_active_users",
        query=f"SELECT * FROM {full_table} WHERE active = true",
        replace=False
    )
    assert result is True
    
    # Drop view
    result = connector.drop_view("test_active_users")
    assert result is True


def test_test_connection(connector):
    """Test connection testing."""
    result = connector.test_connection()
    assert result["status"] == "connected"
    assert "catalog" in result
    assert "namespace" in result


def test_list_tables(connector):
    """Test listing tables."""
    tables = connector.list_tables()
    assert isinstance(tables, list)
    assert "test_users" in tables


def test_get_table_stats(connector):
    """Test getting table statistics."""
    # Insert test data
    connector.insert_row_data(
        "test_users",
        {"id": "stats1", "name": "Stats Test", "email": "stats@test.com", "age": 33, "active": True}
    )
    
    stats = connector.get_table_stats("test_users")
    assert "row_count" in stats
    assert "column_count" in stats
    assert "columns" in stats
    assert stats["table"] == "test_users"


def test_truncate_table(connector):
    """Test truncating a table."""
    # Insert test data
    connector.insert_row_data(
        "test_users",
        {"id": "trunc1", "name": "Truncate Test", "email": "trunc@test.com", "age": 45, "active": False}
    )
    
    # Verify data exists
    data_before = connector.get_table_with_filters("test_users")
    assert len(data_before) > 0
    
    # Truncate table
    result = connector.truncate_table("test_users")
    assert result is True
    
    # Verify data is gone but table exists
    assert connector.table_exists("test_users")
    data_after = connector.get_table_with_filters("test_users")
    assert len(data_after) == 0


def test_upsert_dataframe(connector):
    """Test upsert (insert or update) operation."""
    import pandas as pd
    
    # Insert initial data
    connector.insert_row_data(
        "test_users",
        {"id": "ups1", "name": "Original Name", "email": "original@test.com", "age": 25, "active": True}
    )
    
    # Upsert data (update ups1, insert ups2)
    upsert_df = pd.DataFrame({
        "id": ["ups1", "ups2"],
        "name": ["Updated Name", "New User"],
        "email": ["updated@test.com", "new@test.com"],
        "age": [26, 30],
        "active": [True, True]
    })
    
    result = connector.upsert_dataframe_data(
        table="test_users",
        df=upsert_df,
        id_column="id"
    )
    assert result is True
    
    # Verify update
    updated = connector.fetch_row_by_id("test_users", "ups1")
    assert updated["name"] == "Updated Name"
    assert updated["age"] == 26
    
    # Verify insert
    inserted = connector.fetch_row_by_id("test_users", "ups2")
    assert inserted["name"] == "New User"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
