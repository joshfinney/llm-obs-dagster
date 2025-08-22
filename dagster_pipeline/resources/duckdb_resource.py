from dagster import resource, Field, String
import duckdb
from contextlib import contextmanager

@resource(
    config_schema={
        "database_path": Field(String, default_value=":memory:")
    }
)
def duckdb_resource(context):
    """
    DuckDB resource for efficient analytical queries
    Methods:
    - execute(query, params)
    - create_table_from_df(df, table_name)
    - read_parquet_from_s3(s3_path)
    """
    class DuckDBResource:
        def __init__(self, db_path):
            self.db_path = db_path
            
        @contextmanager
        def get_connection(self):
            conn = duckdb.connect(self.db_path)
            # Install and load httpfs for S3 access
            conn.execute("INSTALL httpfs; LOAD httpfs;")
            conn.execute(f"SET s3_region='{context.resource_config.get('aws_region', 'us-east-1')}';")
            try:
                yield conn
            finally:
                conn.close()
                
        def execute_query(self, query: str, params=None):
            with self.get_connection() as conn:
                return conn.execute(query, params).fetchdf()
    
    return DuckDBResource(context.resource_config["database_path"])