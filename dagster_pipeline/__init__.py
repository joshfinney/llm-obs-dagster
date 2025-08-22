from dagster import Definitions, load_assets_from_modules

from dagster_pipeline.assets import aggregations, metric_enrichment, mlflow_ingestion
from dagster_pipeline.jobs.delta_job import (
    delta_ingestion_job,
    historical_ingestion_job,
)
from dagster_pipeline.resources.api_clients import (
    metrics_api_resource,
    settings_resource,
    thresholds_api_resource,
)
from dagster_pipeline.resources.duckdb_resource import duckdb_resource
from dagster_pipeline.resources.mlflow_resource import mlflow_resource
from dagster_pipeline.resources.s3_io_manager import s3_parquet_io_manager
from dagster_pipeline.schedules.pipeline_schedules import (
    backup_schedule,
    delta_schedule,
    historical_schedule,
)

# Load all assets from modules
all_assets = load_assets_from_modules([
    mlflow_ingestion,
    metric_enrichment,
    aggregations
])

# Define comprehensive Dagster repository
defs = Definitions(
    assets=all_assets,
    jobs=[
        delta_ingestion_job,
        historical_ingestion_job
    ],
    schedules=[
        delta_schedule,
        historical_schedule,
        backup_schedule
    ],
    resources={
        "mlflow": mlflow_resource,
        "duckdb": duckdb_resource,
        "metrics_api": metrics_api_resource,
        "thresholds_api": thresholds_api_resource,
        "settings": settings_resource,
        "s3_parquet_io": s3_parquet_io_manager
    }
)
