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
