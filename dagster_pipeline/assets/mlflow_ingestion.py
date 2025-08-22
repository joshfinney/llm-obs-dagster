from datetime import datetime, timedelta
import logging

from dagster import Output, asset
import pandas as pd
from pydantic import BaseModel

logger = logging.getLogger(__name__)

class MLflowIngestionConfig(BaseModel):
    lookback_hours: int = 1
    max_results_per_page: int = 100
    experiment_filter: str | None = None

@asset(
    required_resource_keys={"mlflow", "settings"},
    io_manager_key="s3_parquet_io",
    compute_kind="mlflow",
    group_name="ingestion",
    description=(
        "Extract runs from MLflow tracking server with configurable time windows"
    )
)
def raw_mlflow_runs(context, config: MLflowIngestionConfig) -> pd.DataFrame:
    """Extract runs from MLflow based on time window.

    For delta jobs: extracts runs from last N hours
    For historical jobs: extracts runs from last N days
    """
    mlflow_client = context.resources.mlflow

    # Calculate time window
    lookback_hours = config.lookback_hours
    lookback_timestamp = int(
        (datetime.now() - timedelta(hours=lookback_hours)).timestamp() * 1000
    )

    context.log.info(f"Extracting MLflow runs from last {lookback_hours} hours")
    context.log.info(f"Lookback timestamp: {lookback_timestamp}")

    # Get all experiments if no filter specified
    if config.experiment_filter:
        experiments = mlflow_client.search_experiments(
            filter_string=config.experiment_filter
        )
    else:
        experiments = mlflow_client.search_experiments()

    experiment_ids = [exp.experiment_id for exp in experiments]
    context.log.info(
        f"Searching {len(experiment_ids)} experiments: "
        f"{[exp.name for exp in experiments]}"
    )

    # Build filter string for time-based filtering
    filter_string = f"attributes.start_time > {lookback_timestamp}"

    # Fetch runs with pagination
    all_runs = []
    page_token = None
    total_pages = 0

    while True:
        try:
            runs = mlflow_client.search_runs(
                experiment_ids=experiment_ids,
                filter_string=filter_string,
                max_results=config.max_results_per_page,
                page_token=page_token
            )

            all_runs.extend(runs)
            total_pages += 1

            context.log.info(f"Fetched page {total_pages} with {len(runs)} runs")

            if not hasattr(runs, 'token') or runs.token is None:
                break
            page_token = runs.token

        except Exception as e:
            context.log.error(f"Error fetching runs: {e!s}")
            raise

    context.log.info(f"Total runs fetched: {len(all_runs)} across {total_pages} pages")

    # Transform runs to structured format
    run_data = []
    for run in all_runs:
        try:
            # Extract experiment info
            experiment = next(
                exp for exp in experiments
                if exp.experiment_id == run.info.experiment_id
            )

            # Flatten metrics - each metric becomes a separate row for easier
            # aggregation
            for metric_name, metric_value in run.data.metrics.items():
                run_record = {
                    'run_id': run.info.run_id,
                    'experiment_id': run.info.experiment_id,
                    'experiment_name': experiment.name,
                    'user_id': run.data.tags.get('mlflow.user', 'unknown'),
                    'run_name': run.data.tags.get(
                        'mlflow.runName', run.info.run_id[:8]
                    ),
                    'start_time': datetime.fromtimestamp(run.info.start_time / 1000),
                    'end_time': (
                        datetime.fromtimestamp(run.info.end_time / 1000)
                        if run.info.end_time else None
                    ),
                    'status': run.info.status,
                    'metric_name': metric_name,
                    'metric_value': metric_value,
                    'params': run.data.params,
                    'tags': run.data.tags,
                    'extraction_timestamp': datetime.now()
                }
                run_data.append(run_record)

        except Exception as e:
            context.log.warning(f"Error processing run {run.info.run_id}: {e!s}")
            continue

    df = pd.DataFrame(run_data)

    if len(df) == 0:
        context.log.warning("No runs found in the specified time window")
        return pd.DataFrame()

    # Data quality checks
    null_metrics = df['metric_value'].isnull().sum()
    if null_metrics > 0:
        context.log.warning(f"Found {null_metrics} null metric values")

    # Log extraction statistics
    stats = {
        "total_records": len(df),
        "unique_runs": df['run_id'].nunique(),
        "unique_experiments": df['experiment_id'].nunique(),
        "unique_metrics": df['metric_name'].nunique(),
        "unique_users": df['user_id'].nunique(),
        "time_range_start": df['start_time'].min().isoformat() if len(df) > 0 else None,
        "time_range_end": df['start_time'].max().isoformat() if len(df) > 0 else None,
        "top_experiments": df['experiment_name'].value_counts().head(5).to_dict(),
        "top_metrics": df['metric_name'].value_counts().head(10).to_dict()
    }

    context.log.info(f"Extraction complete: {stats}")

    yield Output(
        df,
        metadata={
            **stats,
            "preview": df.head().to_dict('records') if len(df) > 0 else []
        }
    )
