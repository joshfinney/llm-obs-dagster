from contextlib import contextmanager
import logging
from typing import Any

from dagster import Field, String, resource
import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

@resource(
    config_schema={
        "database_path": Field(String, default_value=":memory:"),
        "aws_region": Field(String, default_value="us-east-1")
    }
)
def duckdb_resource(context):
    """DuckDB resource for efficient analytical queries with S3 integration.
    """
    class DuckDBResource:
        def __init__(self, db_path: str, aws_region: str):
            self.db_path = db_path
            self.aws_region = aws_region

        @contextmanager
        def get_connection(self):
            conn = duckdb.connect(self.db_path)
            try:
                # Install and configure S3 access
                conn.execute("INSTALL httpfs; LOAD httpfs;")
                conn.execute(f"SET s3_region='{self.aws_region}';")

                # Configure memory settings for large datasets
                conn.execute("SET memory_limit='8GB';")
                conn.execute("SET max_memory='8GB';")
                conn.execute("SET threads=4;")

                yield conn
            except Exception as e:
                logger.error(f"DuckDB connection error: {e!s}")
                raise
            finally:
                conn.close()

        def execute(self, query: str, params: dict[str, Any] | None = None):
            """Execute query and return DuckDB result."""
            with self.get_connection() as conn:
                if params:
                    return conn.execute(query, params)
                return conn.execute(query)

        def register(self, name: str, df: pd.DataFrame):
            """Register DataFrame as a DuckDB table."""
            with self.get_connection() as conn:
                conn.register(name, df)

        def create_table_from_df(self, df: pd.DataFrame, table_name: str):
            """Create persistent table from DataFrame."""
            with self.get_connection() as conn:
                conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")

        def read_parquet_from_s3(self, s3_path: str) -> pd.DataFrame:
            """Read Parquet file from S3."""
            with self.get_connection() as conn:
                return conn.execute(f"SELECT * FROM read_parquet('{s3_path}')").df()

        def write_parquet_to_s3(self, df: pd.DataFrame, s3_path: str):
            """Write DataFrame to S3 as Parquet."""
            with self.get_connection() as conn:
                conn.register('temp_df', df)
                conn.execute(f"COPY temp_df TO '{s3_path}' (FORMAT PARQUET)")

    return DuckDBResource(
        context.resource_config["database_path"],
        context.resource_config["aws_region"]
    )
