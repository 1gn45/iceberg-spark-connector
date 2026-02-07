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
