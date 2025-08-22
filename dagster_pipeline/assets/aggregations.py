@asset(
    ins={"enriched": AssetIn("enriched_metrics")},
    required_resource_keys={"duckdb"},
    io_manager_key="s3_parquet_io"
)
def platform_aggregations(context, enriched: pd.DataFrame) -> pd.DataFrame:
    """
    Create platform-level aggregations
    
    SQL queries using DuckDB:
    1. Traffic light summary (count by status)
    2. Pass rate calculations
    3. Trend analysis (WoW, DoD changes)
    4. Recent runs summary
    5. Top failing metrics
    
    Optimize with:
    - Window functions for moving averages
    - CTEs for complex aggregations
    - Materialized views in DuckDB
    """
    duckdb = context.resources.duckdb
    
    query = """
    WITH status_summary AS (
        SELECT 
            DATE_TRUNC('hour', timestamp) as hour,
            status,
            COUNT(*) as count,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY DATE_TRUNC('hour', timestamp)) as percentage
        FROM enriched
        GROUP BY 1, 2
    ),
    moving_stats AS (
        SELECT *,
            AVG(count) OVER (ORDER BY hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) as ma_24h,
            LAG(count, 24) OVER (ORDER BY hour) as count_24h_ago
        FROM status_summary
    )
    SELECT * FROM moving_stats
    """
    
    return duckdb.execute_query(query)

@asset(
    ins={"enriched": AssetIn("enriched_metrics")},
    required_resource_keys={"duckdb"},
    io_manager_key="s3_parquet_io",
    partitions_def=StaticPartitionsDefinition(
        ["application_timeline", "metric_trends", "user_analytics"]
    )
)
def application_aggregations(context, enriched: pd.DataFrame) -> pd.DataFrame:
    """
    Create application-level aggregations
    
    Aggregations:
    1. Per-app traffic lights
    2. Metric timelines with forecasting
    3. User contribution analysis
    4. Anomaly detection flags
    
    Use DuckDB window functions and aggregates
    """
    pass