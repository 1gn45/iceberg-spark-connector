"""
Spark Session Management for Iceberg
Handles SparkSession creation with Iceberg catalog configuration.
"""

import os
from typing import Optional
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()


def get_spark_session(
    catalog_type: Optional[str] = None,
    catalog_uri: Optional[str] = None,
    warehouse_path: Optional[str] = None,
    aws_region: Optional[str] = None,
    spark_master: Optional[str] = None,
    app_name: str = "IcebergConnector",
    catalog_name: str = "iceberg_catalog",
) -> SparkSession:
    """
    Create and configure a SparkSession with Iceberg support.
    
    Args:
        catalog_type: Type of catalog (hadoop, hive, rest, glue)
        catalog_uri: URI for catalog (Hive metastore, REST endpoint, etc.)
        warehouse_path: Path to Iceberg warehouse
        aws_region: AWS region (for Glue catalog)
        spark_master: Spark master URL (defaults to local[*])
        app_name: Application name
        catalog_name: Name of the Iceberg catalog
        
    Returns:
        SparkSession: Configured Spark session
    """
    # Get values from environment if not provided
    catalog_type = catalog_type or os.getenv("ICEBERG_CATALOG_TYPE", "hadoop")
    warehouse_path = warehouse_path or os.getenv(
        "ICEBERG_WAREHOUSE_PATH", "/tmp/iceberg-warehouse"
    )
    spark_master = spark_master or os.getenv("SPARK_MASTER", "local[*]")
    
    # Start building Spark session
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path)
    )
    
    # Configure catalog based on type
    if catalog_type == "hadoop":
        builder = builder.config(
            f"spark.sql.catalog.{catalog_name}.type", "hadoop"
        )
    
    elif catalog_type == "hive":
        catalog_uri = catalog_uri or os.getenv("ICEBERG_CATALOG_URI")
        if not catalog_uri:
            raise ValueError("Hive catalog requires catalog_uri or ICEBERG_CATALOG_URI env var")
        
        builder = (
            builder
            .config(f"spark.sql.catalog.{catalog_name}.type", "hive")
            .config(f"spark.sql.catalog.{catalog_name}.uri", catalog_uri)
            .config("hive.metastore.uris", catalog_uri)
        )
    
    elif catalog_type == "rest":
        catalog_uri = catalog_uri or os.getenv("ICEBERG_CATALOG_URI")
        if not catalog_uri:
            raise ValueError("REST catalog requires catalog_uri or ICEBERG_CATALOG_URI env var")
        
        builder = (
            builder
            .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
            .config(f"spark.sql.catalog.{catalog_name}.uri", catalog_uri)
        )
    
    elif catalog_type == "glue":
        aws_region = aws_region or os.getenv("AWS_REGION", "us-east-1")
        
        builder = (
            builder
            .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config(f"spark.sql.catalog.{catalog_name}.glue.region", aws_region)
        )
    
    else:
        raise ValueError(f"Unsupported catalog type: {catalog_type}")
    
    # Additional Iceberg configs
    builder = (
        builder
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.defaultCatalog", catalog_name)
    )
    
    # Create session
    spark = builder.getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    return spark
