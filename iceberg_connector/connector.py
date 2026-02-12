"""
Iceberg Database Connector
Core class for database operations using PySpark and Apache Iceberg.
"""

from typing import Dict, Any, List, Optional, Tuple
import uuid
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .session import get_spark_session


class IcebergConnector:
    """
    Main connector class for Apache Iceberg table operations.
    Provides CRUD operations and table management using PySpark.
    """
    
    def __init__(
        self,
        namespace: str,
        catalog: str = "iceberg_catalog",
        user: str = None,
        spark: SparkSession = None,
    ):
        """
        Initialize the Iceberg connector.
        
        Args:
            namespace: Namespace (database) name
            catalog: Catalog name (default: iceberg_catalog)
            user: Username (optional, for logging/tracking)
            spark: Existing SparkSession (optional, will create one if not provided)
        """
        self.spark = spark or get_spark_session(catalog_name=catalog)
        self.namespace = namespace
        self.catalog = catalog
        self.user = user
        self.session_id = str(uuid.uuid4())
        
        # Ensure namespace exists
        self._ensure_namespace()
    
    def _ensure_namespace(self):
        """Create namespace if it doesn't exist."""
        try:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.catalog}.{self.namespace}")
        except Exception as e:
            raise ValueError(f"Error creating namespace: {e}")
    
    def _get_table_name(self, table: str) -> str:
        """Get fully qualified table name."""
        return f"{self.catalog}.{self.namespace}.{table}"
    
    # ============= READ OPERATIONS =============
    
    def fetch_row_by_id(
        self, table: str, row_id: str | int, id_column: str = "id"
    ) -> Dict[str, Any]:
        """
        Fetch a single row by ID.
        
        Args:
            table: Table name
            row_id: ID value to search for
            id_column: Name of the ID column (default: 'id')
            
        Returns:
            Dict[str, Any]: Row data as dictionary
        """
        try:
            full_table = self._get_table_name(table)
            df = self.spark.table(full_table)
            filtered_df = df.filter(F.col(id_column) == row_id)
            
            result = filtered_df.limit(1).toPandas()
            if result.empty:
                raise ValueError(f"No row found with {id_column} = {row_id}")
            
            return result.iloc[0].to_dict()
        except Exception as e:
            raise ValueError(f"Error fetching row by ID: {e}")
    
    def fetch_row_by_field_value(
        self, table: str, field: str, value: Any
    ) -> pd.DataFrame:
        """
        Fetch rows where field matches value.
        
        Args:
            table: Table name
            field: Field name to filter on
            value: Value to match
            
        Returns:
            pd.DataFrame: Matching rows
        """
        try:
            full_table = self._get_table_name(table)
            df = self.spark.table(full_table)
            filtered_df = df.filter(F.col(field) == value)
            return filtered_df.toPandas()
        except Exception as e:
            raise ValueError(f"Error fetching rows by field value: {e}")
    
    def get_table_with_filters(
        self,
        table: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[str] = None,
        order_direction: str = "ASC",
        unique_field: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Fetch rows from a table with dynamic filters, ordering, pagination,
        and optional uniqueness.
        
        Args:
            table: Table name
            filters: Dictionary of filters (field -> value/range/list)
            limit: Max number of rows to return
            offset: Number of rows to skip
            order_by: Column to order by
            order_direction: "ASC" or "DESC"
            unique_field: If provided, return only unique values for this field
            
        Returns:
            pd.DataFrame: Result set
        """
        try:
            full_table = self._get_table_name(table)
            df = self.spark.table(full_table)
            
            # Apply filters
            if filters:
                for key, value in filters.items():
                    column = F.col(key)
                    
                    if isinstance(value, dict):
                        # Date range
                        start = value.get("start_date")
                        end = value.get("end_date")
                        if start and end:
                            df = df.filter((column >= start) & (column <= end))
                    
                    elif isinstance(value, tuple) and len(value) == 2:
                        # Numeric range
                        df = df.filter((column >= value[0]) & (column <= value[1]))
                    
                    elif isinstance(value, list):
                        # IN list
                        df = df.filter(column.isin(value))
                    
                    elif isinstance(value, bool):
                        df = df.filter(column == value)
                    
                    elif isinstance(value, (str, int, float)):
                        # Exact match (case-insensitive for strings)
                        if isinstance(value, str):
                            df = df.filter(F.lower(column) == value.lower())
                        else:
                            df = df.filter(column == value)
            
            # Apply uniqueness on a specific field
            if unique_field:
                window_spec = Window.partitionBy(unique_field).orderBy(unique_field)
                df = (
                    df.withColumn("rn", F.row_number().over(window_spec))
                    .filter(F.col("rn") == 1)
                    .drop("rn")
                )
            
            # Apply ordering
            if order_by:
                if order_direction.upper() == "DESC":
                    df = df.orderBy(F.col(order_by).desc())
                else:
                    df = df.orderBy(F.col(order_by).asc())
            
            # Convert to pandas for offset/limit handling
            if offset or limit:
                pandas_df = df.toPandas()
                start = offset or 0
                end = (start + limit) if limit else None
                return pandas_df.iloc[start:end]
            
            return df.toPandas()
        except Exception as e:
            raise ValueError(f"Error getting table with filters: {e}")
    
    def get_filtered_rows_count(
        self,
        table: str,
        filters: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Return the count of rows matching the filters.
        
        Args:
            table: Table name
            filters: Dictionary of filters
            
        Returns:
            int: Count of matching rows
        """
        try:
            full_table = self._get_table_name(table)
            df = self.spark.table(full_table)
            
            # Apply filters (same logic as get_table_with_filters)
            if filters:
                for key, value in filters.items():
                    column = F.col(key)
                    
                    if isinstance(value, dict):
                        start = value.get("start_date")
                        end = value.get("end_date")
                        if start and end:
                            df = df.filter((column >= start) & (column <= end))
                    
                    elif isinstance(value, tuple) and len(value) == 2:
                        df = df.filter((column >= value[0]) & (column <= value[1]))
                    
                    elif isinstance(value, list):
                        df = df.filter(column.isin(value))
                    
                    elif isinstance(value, bool):
                        df = df.filter(column == value)
                    
                    elif isinstance(value, (str, int, float)):
                        if isinstance(value, str):
                            df = df.filter(F.lower(column).like(f"%{value.lower()}%"))
                        else:
                            df = df.filter(column == value)
            
            return df.count()
        except Exception as e:
            raise ValueError(f"Error counting filtered rows: {e}")
    
    def get_column_values(
        self, table: str, column_name: str
    ) -> List[Any]:
        """
        Get all values from a specific column.
        
        Args:
            table: Table name
            column_name: Column name
            
        Returns:
            List[Any]: List of column values
        """
        try:
            full_table = self._get_table_name(table)
            df = self.spark.table(full_table)
            values = df.select(column_name).toPandas()[column_name].tolist()
            return values
        except Exception as e:
            raise ValueError(f"Error getting column values: {e}")
    
    def get_unique_column_values(
        self, table: str, column_name: str
    ) -> List[Any]:
        """
        Get unique values from a specific column, sorted.
        
        Args:
            table: Table name
            column_name: Column name
            
        Returns:
            List[Any]: Sorted list of unique values
        """
        try:
            column_values = self.get_column_values(table, column_name)
            unique_values = list(set(column_values))
            unique_values.sort(key=lambda x: (x is None, x))
            return unique_values
        except Exception as e:
            raise ValueError(f"Error getting unique column values: {e}")
    
    # ============= WRITE OPERATIONS =============
    
    def insert_row_data(
        self, table: str, row_data: dict
    ) -> bool:
        """
        Insert a row into the specified table.
        
        Args:
            table: Table name
            row_data: Dictionary of column -> value
            
        Returns:
            bool: True if successful
        """
        try:
            full_table = self._get_table_name(table)
            
            # Convert to DataFrame
            df = self.spark.createDataFrame([row_data])
            
            # Append to table
            df.writeTo(full_table).append()
            
            return True
        except Exception as e:
            raise ValueError(f"Error inserting row: {e}")
    
    def update_row_data(
        self,
        table: str,
        row_id: str | int,
        updated_values: Dict[str, Any],
        id_column: str = "id",
    ) -> bool:
        """
        Update a row using Iceberg MERGE operation.
        
        Args:
            table: Table name
            row_id: ID of the row to update
            updated_values: Dictionary of column -> new value
            id_column: Name of the ID column (default: 'id')
            
        Returns:
            bool: True if successful
        """
        try:
            full_table = self._get_table_name(table)
            
            # Add ID to updated values
            updated_values[id_column] = row_id
            
            # Create update DataFrame
            update_df = self.spark.createDataFrame([updated_values])
            update_df.createOrReplaceTempView("updates")
            
            # Build SET clause
            set_clause = ", ".join(
                f"t.{col} = s.{col}"
                for col in updated_values.keys()
                if col != id_column
            )
            
            # Execute MERGE
            merge_sql = f"""
                MERGE INTO {full_table} t
                USING updates s
                ON t.{id_column} = s.{id_column}
                WHEN MATCHED THEN UPDATE SET {set_clause}
            """
            
            self.spark.sql(merge_sql)
            return True
        except Exception as e:
            raise ValueError(f"Error updating row: {e}")
    
    def update_dataframe_data(
        self,
        table: str,
        df_updates: pd.DataFrame | None = None,
        df_inserts: pd.DataFrame | None = None,
        id_column: str = "id",
    ) -> bool:
        """
        Update multiple rows from DataFrames (updates by ID, inserts new rows).
        Uses Iceberg MERGE for atomic operations.
        
        Args:
            table: Table name
            df_updates: DataFrame with rows to update (must include ID column)
            df_inserts: DataFrame with new rows to insert
            id_column: Name of the ID column (default: 'id')
            
        Returns:
            bool: True if successful
        """
        try:
            full_table = self._get_table_name(table)
            
            # Handle updates
            if df_updates is not None and not df_updates.empty:
                if id_column not in df_updates.columns:
                    raise ValueError(f"df_updates must include {id_column} column")
                
                # Convert to Spark DataFrame
                update_spark_df = self.spark.createDataFrame(df_updates)
                update_spark_df.createOrReplaceTempView("batch_updates")
                
                # Get columns to update (excluding ID)
                update_cols = [col for col in df_updates.columns if col != id_column]
                set_clause = ", ".join(f"t.{col} = s.{col}" for col in update_cols)
                
                # Execute MERGE for updates
                merge_sql = f"""
                    MERGE INTO {full_table} t
                    USING batch_updates s
                    ON t.{id_column} = s.{id_column}
                    WHEN MATCHED THEN UPDATE SET {set_clause}
                """
                self.spark.sql(merge_sql)
            
            # Handle inserts
            if df_inserts is not None and not df_inserts.empty:
                insert_spark_df = self.spark.createDataFrame(df_inserts)
                insert_spark_df.writeTo(full_table).append()
            
            return True
        except Exception as e:
            raise ValueError(f"Error updating dataframe data: {e}")
    
    def delete_row(
        self, table: str, row_id: str | int, id_column: str = "id"
    ) -> bool:
        """
        Delete a row by ID.
        
        Args:
            table: Table name
            row_id: ID value to delete
            id_column: Name of the ID column (default: 'id')
            
        Returns:
            bool: True if successful
        """
        try:
            full_table = self._get_table_name(table)
            
            # Use Iceberg DELETE
            delete_sql = f"DELETE FROM {full_table} WHERE {id_column} = '{row_id}'"
            self.spark.sql(delete_sql)
            
            return True
        except Exception as e:
            raise ValueError(f"Error deleting row: {e}")
    
    def upsert_dataframe_data(
        self,
        table: str,
        df: pd.DataFrame,
        id_column: str = "id",
    ) -> bool:
        """
        Upsert (insert or update) data from a DataFrame using Iceberg MERGE.
        Updates existing rows and inserts new rows in a single operation.
        
        Args:
            table: Table name
            df: pandas DataFrame with data to upsert
            id_column: Name of the ID column for matching (default: 'id')
            
        Returns:
            bool: True if successful
        """
        try:
            full_table = self._get_table_name(table)
            
            if id_column not in df.columns:
                raise ValueError(f"DataFrame must include {id_column} column")
            
            # Convert to Spark DataFrame
            spark_df = self.spark.createDataFrame(df)
            spark_df.createOrReplaceTempView("upsert_data")
            
            # Get columns to update (excluding ID)
            update_cols = [col for col in df.columns if col != id_column]
            set_clause = ", ".join(f"t.{col} = s.{col}" for col in update_cols)
            
            # Get all columns for insert
            insert_cols = ", ".join(df.columns)
            insert_values = ", ".join(f"s.{col}" for col in df.columns)
            
            # Execute MERGE (upsert)
            merge_sql = f"""
                MERGE INTO {full_table} t
                USING upsert_data s
                ON t.{id_column} = s.{id_column}
                WHEN MATCHED THEN UPDATE SET {set_clause}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values})
            """
            
            self.spark.sql(merge_sql)
            return True
        except Exception as e:
            raise ValueError(f"Error upserting dataframe data: {e}")
    
    # ============= UTILITY OPERATIONS =============
    
    def run_sql(self, query: str) -> pd.DataFrame:
        """
        Execute raw SQL query.
        
        Args:
            query: SQL query string
            
        Returns:
            pd.DataFrame: Query results
        """
        try:
            result = self.spark.sql(query)
            return result.toPandas()
        except Exception as e:
            raise ValueError(f"Error running SQL: {e}")
    
    def table_exists(self, table: str) -> bool:
        """
        Check if a table exists.
        
        Args:
            table: Table name
            
        Returns:
            bool: True if table exists
        """
        try:
            full_table = self._get_table_name(table)
            self.spark.table(full_table)
            return True
        except Exception:
            return False
    
    def create_namespace(self, namespace: str):
        """
        Create a new namespace (database).
        
        Args:
            namespace: Namespace name
        """
        try:
            sql = f"CREATE NAMESPACE IF NOT EXISTS {self.catalog}.{namespace}"
            self.spark.sql(sql)
            return True
        except Exception as e:
            raise ValueError(f"Error creating namespace: {e}")
    
    def get_table_schema(self, table: str) -> List[Tuple[str, str]]:
        """
        Get table schema (column names and types).
        
        Args:
            table: Table name
            
        Returns:
            List[Tuple[str, str]]: List of (column_name, data_type) tuples
        """
        try:
            full_table = self._get_table_name(table)
            df = self.spark.table(full_table)
            schema = [(field.name, str(field.dataType)) for field in df.schema.fields]
            return schema
        except Exception as e:
            raise ValueError(f"Error getting table schema: {e}")
    
    def get_table_history(self, table: str) -> pd.DataFrame:
        """
        Get Iceberg table snapshot history.
        
        Args:
            table: Table name
            
        Returns:
            pd.DataFrame: Snapshot history
        """
        try:
            full_table = self._get_table_name(table)
            history_sql = f"SELECT * FROM {full_table}.history"
            return self.spark.sql(history_sql).toPandas()
        except Exception as e:
            raise ValueError(f"Error getting table history: {e}")
    
    def read_table_at_snapshot(
        self, table: str, snapshot_id: int
    ) -> pd.DataFrame:
        """
        Time-travel query: read table at a specific snapshot.
        
        Args:
            table: Table name
            snapshot_id: Snapshot ID to query
            
        Returns:
            pd.DataFrame: Table data at that snapshot
        """
        try:
            full_table = self._get_table_name(table)
            df = self.spark.read.option("snapshot-id", snapshot_id).table(full_table)
            return df.toPandas()
        except Exception as e:
            raise ValueError(f"Error reading table at snapshot: {e}")
    
    def remove_duplicate_rows(
        self, table: str, columns: List[str], id_column: str = "id"
    ) -> str:
        """
        Remove duplicate rows based on specified columns.
        Keeps the first occurrence based on ID column.
        
        Args:
            table: Table name
            columns: List of column names to check for duplicates
            id_column: ID column to determine which row to keep (default: 'id')
            
        Returns:
            str: Status message
        """
        try:
            full_table = self._get_table_name(table)
            df = self.spark.table(full_table)
            
            # Window: partition by columns, order by ID
            window_spec = Window.partitionBy(*columns).orderBy(id_column)
            
            # Add row number
            df_with_rn = df.withColumn("rn", F.row_number().over(window_spec))
            
            # Get duplicate IDs (rn > 1)
            duplicates = df_with_rn.filter(F.col("rn") > 1).select(id_column)
            duplicate_ids = [row[id_column] for row in duplicates.collect()]
            
            if not duplicate_ids:
                return "No duplicates found."
            
            # Delete duplicates
            ids_str = ", ".join(f"'{id}'" for id in duplicate_ids)
            delete_sql = f"DELETE FROM {full_table} WHERE {id_column} IN ({ids_str})"
            self.spark.sql(delete_sql)
            
            return f"Deleted {len(duplicate_ids)} duplicate rows."
        except Exception as e:
            raise ValueError(f"Error removing duplicate rows: {e}")
    
    # ============= FILE OPERATIONS =============
    
    def load_data_from_file(
        self,
        table: str,
        file_path: str,
        file_format: str = "csv",
        mode: str = "append",
        options: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        Load data from a file (CSV, JSON, Parquet) into an Iceberg table.
        
        Args:
            table: Table name
            file_path: Path to the file (local or remote)
            file_format: File format ('csv', 'json', 'parquet')
            mode: Write mode ('append', 'overwrite', 'error', 'ignore')
            options: Additional options for reading the file (e.g., {'header': 'true', 'delimiter': ','})
            
        Returns:
            bool: True if successful
        """
        try:
            full_table = self._get_table_name(table)
            
            # Default options for CSV
            if options is None and file_format == "csv":
                options = {"header": "true", "inferSchema": "true"}
            
            # Read file
            reader = self.spark.read.format(file_format)
            if options:
                for key, value in options.items():
                    reader = reader.option(key, value)
            
            df = reader.load(file_path)
            
            # Check if table exists
            table_exists = self.table_exists(table)
            
            # Write to table
            if table_exists:
                # Table exists, use append or overwrite
                if mode == "overwrite":
                    df.writeTo(full_table).using("iceberg").overwritePartitions()
                else:
                    df.writeTo(full_table).append()
            else:
                # Table doesn't exist, create it
                df.writeTo(full_table).using("iceberg").create()
            
            return True
        except Exception as e:
            raise ValueError(f"Error loading data from file: {e}")
    
    def export_table_to_file(
        self,
        table: str,
        file_path: str,
        file_format: str = "csv",
        mode: str = "overwrite",
        options: Optional[Dict[str, str]] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Export table data to a file (CSV, JSON, Parquet).
        
        Args:
            table: Table name
            file_path: Output file path
            file_format: File format ('csv', 'json', 'parquet')
            mode: Write mode ('overwrite', 'append', 'ignore', 'error')
            options: Additional write options (e.g., {'header': 'true'})
            filters: Optional filters to apply before export
            
        Returns:
            bool: True if successful
        """
        try:
            full_table = self._get_table_name(table)
            df = self.spark.table(full_table)
            
            # Apply filters if provided
            if filters:
                for key, value in filters.items():
                    column = F.col(key)
                    
                    if isinstance(value, dict):
                        start = value.get("start_date")
                        end = value.get("end_date")
                        if start and end:
                            df = df.filter((column >= start) & (column <= end))
                    
                    elif isinstance(value, tuple) and len(value) == 2:
                        df = df.filter((column >= value[0]) & (column <= value[1]))
                    
                    elif isinstance(value, list):
                        df = df.filter(column.isin(value))
                    
                    elif isinstance(value, bool):
                        df = df.filter(column == value)
                    
                    elif isinstance(value, (str, int, float)):
                        if isinstance(value, str):
                            df = df.filter(F.lower(column) == value.lower())
                        else:
                            df = df.filter(column == value)
            
            # Default options for CSV
            if options is None and file_format == "csv":
                options = {"header": "true"}
            
            # Write file
            writer = df.write.format(file_format).mode(mode)
            if options:
                for key, value in options.items():
                    writer = writer.option(key, value)
            
            writer.save(file_path)
            
            return True
        except Exception as e:
            raise ValueError(f"Error exporting table to file: {e}")
    
    # ============= TABLE MANAGEMENT =============
    
    def create_table_from_dataframe(
        self,
        table: str,
        df: pd.DataFrame,
        partition_by: Optional[List[str]] = None,
    ) -> bool:
        """
        Create a new Iceberg table from a pandas DataFrame.
        
        Args:
            table: Table name
            df: pandas DataFrame with data
            partition_by: List of column names to partition by
            
        Returns:
            bool: True if successful
        """
        try:
            full_table = self._get_table_name(table)
            
            # Convert to Spark DataFrame
            spark_df = self.spark.createDataFrame(df)
            
            # Create table
            writer = spark_df.writeTo(full_table).using("iceberg")
            
            if partition_by:
                writer = writer.partitionedBy(*partition_by)
            
            writer.create()
            
            return True
        except Exception as e:
            raise ValueError(f"Error creating table from DataFrame: {e}")
    
    def clone_table(
        self,
        source_table: str,
        target_table: str,
        include_data: bool = True,
    ) -> bool:
        """
        Clone an existing table (schema and optionally data).
        
        Args:
            source_table: Source table name
            target_table: Target table name
            include_data: If True, copy data; if False, only copy schema
            
        Returns:
            bool: True if successful
        """
        try:
            source_full = self._get_table_name(source_table)
            target_full = self._get_table_name(target_table)
            
            # Read source table
            df = self.spark.table(source_full)
            
            if include_data:
                # Copy with data
                df.writeTo(target_full).using("iceberg").create()
            else:
                # Copy only schema (create empty table)
                df.limit(0).writeTo(target_full).using("iceberg").create()
            
            return True
        except Exception as e:
            raise ValueError(f"Error cloning table: {e}")
    
    def drop_table(self, table: str, purge: bool = False) -> bool:
        """
        Drop a table.
        
        Args:
            table: Table name
            purge: If True, also delete underlying data files
            
        Returns:
            bool: True if successful
        """
        try:
            full_table = self._get_table_name(table)
            
            if purge:
                self.spark.sql(f"DROP TABLE IF EXISTS {full_table} PURGE")
            else:
                self.spark.sql(f"DROP TABLE IF EXISTS {full_table}")
            
            return True
        except Exception as e:
            raise ValueError(f"Error dropping table: {e}")
    
    def truncate_table(self, table: str) -> bool:
        """
        Truncate a table (delete all data but keep schema).
        
        Args:
            table: Table name
            
        Returns:
            bool: True if successful
        """
        try:
            full_table = self._get_table_name(table)
            self.spark.sql(f"TRUNCATE TABLE {full_table}")
            return True
        except Exception as e:
            raise ValueError(f"Error truncating table: {e}")
    
    # ============= SCHEMA OPERATIONS =============
    
    def add_column(
        self,
        table: str,
        column_name: str,
        column_type: str,
        comment: Optional[str] = None,
    ) -> bool:
        """
        Add a new column to an existing table.
        
        Args:
            table: Table name
            column_name: Name of the new column
            column_type: Data type (e.g., 'STRING', 'INT', 'TIMESTAMP')
            comment: Optional column comment
            
        Returns:
            bool: True if successful
        """
        try:
            full_table = self._get_table_name(table)
            
            sql = f"ALTER TABLE {full_table} ADD COLUMN {column_name} {column_type}"
            if comment:
                sql += f" COMMENT '{comment}'"
            
            self.spark.sql(sql)
            return True
        except Exception as e:
            raise ValueError(f"Error adding column: {e}")
    
    def drop_column(self, table: str, column_name: str) -> bool:
        """
        Drop a column from a table.
        
        Args:
            table: Table name
            column_name: Name of the column to drop
            
        Returns:
            bool: True if successful
        """
        try:
            full_table = self._get_table_name(table)
            self.spark.sql(f"ALTER TABLE {full_table} DROP COLUMN {column_name}")
            return True
        except Exception as e:
            raise ValueError(f"Error dropping column: {e}")
    
    def rename_column(
        self, table: str, old_name: str, new_name: str
    ) -> bool:
        """
        Rename a column in a table.
        
        Args:
            table: Table name
            old_name: Current column name
            new_name: New column name
            
        Returns:
            bool: True if successful
        """
        try:
            full_table = self._get_table_name(table)
            self.spark.sql(
                f"ALTER TABLE {full_table} RENAME COLUMN {old_name} TO {new_name}"
            )
            return True
        except Exception as e:
            raise ValueError(f"Error renaming column: {e}")
    
    # ============= VIEW OPERATIONS =============
    
    def create_view(
        self,
        view_name: str,
        query: str,
        replace: bool = False,
    ) -> bool:
        """
        Create a view from a SQL query.
        
        Args:
            view_name: Name of the view
            query: SQL query for the view
            replace: If True, replace existing view
            
        Returns:
            bool: True if successful
        """
        try:
            full_view = self._get_table_name(view_name)
            
            if replace:
                sql = f"CREATE OR REPLACE VIEW {full_view} AS {query}"
            else:
                sql = f"CREATE VIEW {full_view} AS {query}"
            
            self.spark.sql(sql)
            return True
        except Exception as e:
            raise ValueError(f"Error creating view: {e}")
    
    def drop_view(self, view_name: str) -> bool:
        """
        Drop a view.
        
        Args:
            view_name: Name of the view to drop
            
        Returns:
            bool: True if successful
        """
        try:
            full_view = self._get_table_name(view_name)
            self.spark.sql(f"DROP VIEW IF EXISTS {full_view}")
            return True
        except Exception as e:
            raise ValueError(f"Error dropping view: {e}")
    
    # ============= UTILITY OPERATIONS =============
    
    def test_connection(self) -> Dict[str, Any]:
        """
        Test the connection and return session information.
        
        Returns:
            Dict[str, Any]: Connection status and session info
        """
        try:
            # Try to execute a simple query
            result = self.spark.sql("SELECT 1 as test").collect()
            
            return {
                "status": "connected",
                "catalog": self.catalog,
                "namespace": self.namespace,
                "spark_version": self.spark.version,
                "session_id": self.session_id,
            }
        except Exception as e:
            return {
                "status": "failed",
                "error": str(e),
            }
    
    def list_tables(self, namespace: Optional[str] = None) -> List[str]:
        """
        List all tables in a namespace.
        
        Args:
            namespace: Namespace to list tables from (defaults to current namespace)
            
        Returns:
            List[str]: List of table names
        """
        try:
            ns = namespace or self.namespace
            tables_df = self.spark.sql(f"SHOW TABLES IN {self.catalog}.{ns}")
            table_names = [row["tableName"] for row in tables_df.collect()]
            return table_names
        except Exception as e:
            raise ValueError(f"Error listing tables: {e}")
    
    def create_table_with_sql(
        self,
        table: str,
        schema: str,
        partition_by: Optional[str] = None,
    ) -> bool:
        """
        Create an Iceberg table using SQL DDL.
        
        Args:
            table: Table name
            schema: Column definitions (e.g., "id STRING, name STRING, age INT")
            partition_by: Partition specification (e.g., "days(timestamp)")
            
        Returns:
            bool: True if successful
        """
        try:
            full_table = self._get_table_name(table)
            
            sql = f"CREATE TABLE IF NOT EXISTS {full_table} ({schema}) USING iceberg"
            
            if partition_by:
                sql += f" PARTITIONED BY ({partition_by})"
            
            self.spark.sql(sql)
            return True
        except Exception as e:
            raise ValueError(f"Error creating table: {e}")
    
    def get_table_stats(self, table: str) -> Dict[str, Any]:
        """
        Get statistics about a table.
        
        Args:
            table: Table name
            
        Returns:
            Dict[str, Any]: Table statistics
        """
        try:
            full_table = self._get_table_name(table)
            df = self.spark.table(full_table)
            
            row_count = df.count()
            schema = [(field.name, str(field.dataType)) for field in df.schema.fields]
            
            return {
                "table": table,
                "row_count": row_count,
                "column_count": len(schema),
                "columns": schema,
            }
        except Exception as e:
            raise ValueError(f"Error getting table stats: {e}")
