@asset(
    ins={"raw_runs": AssetIn("raw_mlflow_runs")},
    required_resource_keys={"metrics_api", "thresholds_api"},
    io_manager_key="s3_parquet_io"
)
def enriched_metrics(context, raw_runs: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich metrics with metadata and thresholds
    
    Implementation:
    1. Extract unique metrics from runs
    2. Batch fetch metric metadata from API
    3. Fetch threshold configurations (platform + experiment level)
    4. Apply threshold logic (experiment overrides platform)
    5. Calculate metric status (critical/warning/healthy)
    6. Add forecasting data (using statsmodels or prophet-lite)
    
    Libraries: requests, pandas, statsmodels
    """
    metrics_client = context.resources.metrics_api
    thresholds_client = context.resources.thresholds_api
    
    # Get unique metrics
    unique_metrics = raw_runs['metric_name'].unique()
    
    # Batch fetch metadata with retry logic
    metadata = metrics_client.batch_get_metadata(unique_metrics)
    
    # Fetch thresholds with caching
    thresholds = thresholds_client.get_thresholds(
        metrics=unique_metrics,
        experiments=raw_runs['experiment_id'].unique()
    )
    
    # Apply business logic for status calculation
    df['status'] = df.apply(calculate_status, axis=1, thresholds=thresholds)
    
    return df