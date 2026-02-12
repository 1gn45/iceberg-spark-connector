"""
Advanced usage examples for Iceberg Spark Connector
Demonstrates file operations, schema evolution, and table management
"""

from iceberg_connector import IcebergConnector
import pandas as pd
import os

# Initialize connector
conn = IcebergConnector(
    namespace="advanced_demo",
    catalog="iceberg_catalog"
)

print("=" * 60)
print("Advanced Iceberg Connector Examples")
print("=" * 60)

# ============= 1. File Operations =============
print("\n1. FILE OPERATIONS")
print("-" * 60)

# Create sample CSV data
print("Creating sample CSV file...")
csv_data = """id,product,price,quantity,category
p001,Laptop,999.99,10,Electronics
p002,Mouse,29.99,50,Electronics
p003,Keyboard,79.99,30,Electronics
p004,Desk Chair,199.99,15,Furniture
p005,Monitor,299.99,20,Electronics"""

csv_file = "/tmp/products.csv"
with open(csv_file, "w") as f:
    f.write(csv_data)

# Create table and load from CSV
print("Creating products table from CSV...")
conn.spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg_catalog.advanced_demo.products (
        id STRING,
        product STRING,
        price DOUBLE,
        quantity INT,
        category STRING
    ) USING iceberg
""")

conn.load_data_from_file(
    table="products",
    file_path=csv_file,
    file_format="csv",
    mode="append"
)
print("✓ Loaded 5 products from CSV")

# Export filtered data
print("\nExporting electronics to CSV...")
conn.export_table_to_file(
    table="products",
    file_path="/tmp/electronics.csv",
    file_format="csv",
    filters={"category": "Electronics"}
)
print("✓ Exported electronics to /tmp/electronics.csv")

# ============= 2. Table Management =============
print("\n2. TABLE MANAGEMENT")
print("-" * 60)

# Create table from DataFrame
print("Creating customers table from DataFrame...")
customers_df = pd.DataFrame({
    "customer_id": ["c001", "c002", "c003"],
    "name": ["Alice Johnson", "Bob Smith", "Carol Williams"],
    "email": ["alice@example.com", "bob@example.com", "carol@example.com"],
    "country": ["USA", "UK", "Canada"],
    "lifetime_value": [1500.0, 2300.0, 1800.0]
})

conn.create_table_from_dataframe(
    table="customers",
    df=customers_df
)
print(f"✓ Created customers table with {len(customers_df)} rows")

# Clone table
print("\nCloning customers table for backup...")
conn.clone_table(
    source_table="customers",
    target_table="customers_backup",
    include_data=True
)
print("✓ Created customers_backup")

# Get table statistics
print("\nGetting table statistics...")
stats = conn.get_table_stats("products")
print(f"  - Rows: {stats['row_count']}")
print(f"  - Columns: {stats['column_count']}")

# ============= 3. Schema Evolution =============
print("\n3. SCHEMA EVOLUTION")
print("-" * 60)

# Add new column
print("Adding 'supplier' column to products table...")
conn.add_column(
    table="products",
    column_name="supplier",
    column_type="STRING",
    comment="Product supplier name"
)
print("✓ Column added")

# Update some rows with supplier info
print("\nUpdating products with supplier information...")
conn.update_row_data(
    table="products",
    row_id="p001",
    updated_values={"supplier": "TechCorp"}
)
conn.update_row_data(
    table="products",
    row_id="p002",
    updated_values={"supplier": "AccessoriesInc"}
)
print("✓ Updated 2 products")

# Show schema
schema = conn.get_table_schema("products")
print("\nCurrent products schema:")
for col_name, col_type in schema:
    print(f"  - {col_name}: {col_type}")

# Rename column
print("\nRenaming 'quantity' to 'stock_level'...")
conn.rename_column(
    table="products",
    old_name="quantity",
    new_name="stock_level"
)
print("✓ Column renamed")

# ============= 4. View Operations =============
print("\n4. VIEW OPERATIONS")
print("-" * 60)

# Create a view for low stock products
print("Creating low_stock_view...")
full_table = conn.get_table_name("products")
conn.create_view(
    view_name="low_stock_products",
    query=f"SELECT id, product, stock_level FROM {full_table} WHERE stock_level < 20",
    replace=True
)
print("✓ View created")

# Query the view
print("\nQuerying low_stock_products view...")
low_stock = conn.run_sql(
    f"SELECT * FROM {conn.get_table_name('low_stock_products')}"
)
print(f"Found {len(low_stock)} products with low stock:")
print(low_stock[["id", "product", "stock_level"]])

# ============= 5. Advanced Queries =============
print("\n5. ADVANCED QUERIES")
print("-" * 60)

# Get unique categories
print("Getting unique product categories...")
categories = conn.get_unique_column_values(
    table="products",
    column_name="category"
)
print(f"Categories: {categories}")

# Get products with price range
print("\nGetting products with price between $50 and $300...")
mid_range_products = conn.get_table_with_filters(
    table="products",
    filters={
        "price": (50.0, 300.0)
    },
    order_by="price",
    order_direction="ASC"
)
print(f"Found {len(mid_range_products)} mid-range products")
print(mid_range_products[["id", "product", "price"]])

# ============= 6. Connection & Metadata =============
print("\n6. CONNECTION & METADATA")
print("-" * 60)

# Test connection
print("Testing connection...")
conn_info = conn.test_connection()
print(f"Status: {conn_info['status']}")
print(f"Catalog: {conn_info['catalog']}")
print(f"Namespace: {conn_info['namespace']}")
print(f"Spark Version: {conn_info['spark_version']}")

# List all tables
print("\nListing all tables in namespace...")
tables = conn.list_tables()
print(f"Tables: {', '.join(tables)}")

# ============= 7. Cleanup =============
print("\n7. CLEANUP (Optional)")
print("-" * 60)
print("Note: Uncomment below to clean up test data")
# conn.drop_view("low_stock_products")
# conn.drop_table("products", purge=True)
# conn.drop_table("customers", purge=True)
# conn.drop_table("customers_backup", purge=True)
# os.remove(csv_file)
# os.remove("/tmp/electronics.csv")

print("\n" + "=" * 60)
print("✅ All advanced examples completed successfully!")
print("=" * 60)
