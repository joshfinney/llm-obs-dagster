from dagster import asset, AssetIn, Output, AssetMaterialization
import pandas as pd
from typing import List
import logging

logger = logging.getLogger(__name__)

@asset(
    required_resource_keys={"mlflow", "duckdb"},
    io_manager_key="s3_parquet_io",
    compute_kind="mlflow",
    metadata={"partition": "hourly"}
)
def raw_mlflow_runs(context) -> pd.DataFrame:
    """
    Extract runs from MLflow based on time window
    
    Implementation:
    1. Calculate time window (delta vs historical mode)
    2. Query MLflow for runs: client.search_runs()
    3. Extract metrics, params, tags
    4. Flatten nested structure
    5. Add extraction metadata
    6. Log statistics
    
    Libraries: mlflow, pandas
    """
    mlflow_client = context.resources.mlflow
    
    # Determine lookback based on job type
    lookback_hours = context.op_config.get("lookback_hours", 1)
    
    # Build filter string
    filter_string = f"attributes.start_time > {lookback_timestamp}"
    
    # Fetch runs with pagination
    all_runs = []
    page_token = None
    
    while True:
        runs = mlflow_client.search_runs(
            experiment_ids=experiment_ids,
            filter_string=filter_string,
            max_results=100,
            page_token=page_token
        )
        all_runs.extend(runs)
        if not runs.token:
            break
        page_token = runs.token
    
    # Transform to DataFrame
    df = pd.DataFrame([...])
    
    context.log.info(f"Extracted {len(df)} runs")
    
    yield Output(
        df,
        metadata={
            "num_runs": len(df),
            "experiments": df['experiment_name'].unique().tolist(),
            "time_range": f"{lookback_timestamp} to now"
        }
    )