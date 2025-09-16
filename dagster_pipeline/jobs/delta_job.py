from dagster import RunConfig, job

from dagster_pipeline.assets.aggregations import (
    application_summary,
    metric_timeseries,
    platform_summary,
)
from dagster_pipeline.assets.metric_enrichment import enriched_metrics
from dagster_pipeline.assets.mlflow_ingestion import raw_mlflow_runs
from dagster_pipeline.resources.api_clients import (
    metrics_api_resource,
    settings_resource,
    thresholds_api_resource,
)
from dagster_pipeline.resources.duckdb_resource import duckdb_resource
from dagster_pipeline.resources.mlflow_resource import mlflow_resource
from dagster_pipeline.resources.io_manager import get_parquet_io_manager


@job(
    name="delta_ingestion_job",
    description="Incremental pipeline running every 5 minutes for real-time monitoring",
    resource_defs={
        "mlflow": mlflow_resource,
        "duckdb": duckdb_resource,
        "metrics_api": metrics_api_resource,
        "thresholds_api": thresholds_api_resource,
        "settings": settings_resource,
        "s3_parquet_io": get_parquet_io_manager()
    },
    config=RunConfig(
        ops={
            "raw_mlflow_runs": {
                "config": {
                    "lookback_hours": 1,  # Look back 1 hour for delta processing
                    "max_results_per_page": 100
                }
            }
        }
    )
)
def delta_ingestion_job():
    """Delta processing job for real-time monitoring.

    Pipeline:
    1. Extract runs from last hour from MLflow
    2. Enrich with enterprise API metadata/thresholds
    3. Create platform, application, and timeseries aggregations
    4. Persist to S3 for Streamlit consumption

    Runs every 5 minutes via schedule
    """
    # Extract fresh MLflow data
    raw_runs = raw_mlflow_runs()

    # Enrich with metadata and calculate status
    enriched = enriched_metrics(raw_runs)

    # Create aggregations for different views
    platform_agg = platform_summary(enriched)
    app_agg = application_summary(enriched)
    timeseries = metric_timeseries(enriched)

    return [platform_agg, app_agg, timeseries]

@job(
    name="historical_ingestion_job",
    description="Historical data processing for backfill and daily refresh",
    resource_defs={
        "mlflow": mlflow_resource,
        "duckdb": duckdb_resource,
        "metrics_api": metrics_api_resource,
        "thresholds_api": thresholds_api_resource,
        "settings": settings_resource,
        "s3_parquet_io": get_parquet_io_manager()
    },
    config=RunConfig(
        ops={
            "raw_mlflow_runs": {
                "config": {
                    "lookback_hours": 720,  # Look back 30 days for historical processing
                    "max_results_per_page": 500
                }
            }
        }
    )
)
def historical_ingestion_job():
    """Historical processing job for full data refresh.

    Pipeline:
    1. Extract runs from last 30 days from MLflow
    2. Enrich with enterprise API metadata/thresholds
    3. Create comprehensive aggregations
    4. Persist to S3 with full historical context

    Runs daily at 2 AM via schedule
    """
    # Extract historical MLflow data
    raw_runs = raw_mlflow_runs()

    # Enrich with metadata and calculate status
    enriched = enriched_metrics(raw_runs)

    # Create comprehensive aggregations
    platform_agg = platform_summary(enriched)
    app_agg = application_summary(enriched)
    timeseries = metric_timeseries(enriched)

    return [platform_agg, app_agg, timeseries]
