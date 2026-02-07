"""
Apache Iceberg Spark Connector
A clean, minimal connector for Apache Iceberg tables using PySpark.
"""

from .connector import IcebergConnector
from .session import get_spark_session

__version__ = "0.1.0"
__all__ = ["IcebergConnector", "get_spark_session"]
