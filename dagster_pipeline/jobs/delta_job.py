from dagster import job, op, RunRequest, sensor, SkipReason
from dagster_aws.s3 import s3_pickle_io_manager

@job(
    resource_defs={
        "mlflow": mlflow_resource,
        "duckdb": duckdb_resource,
        "s3": s3_resource,
        "metrics_api": metrics_api_client,
        "thresholds_api": thresholds_api_client,
        "io_manager": s3_pickle_io_manager
    },
    config={
        "ops": {
            "raw_mlflow_runs": {
                "config": {"lookback_hours": 1}
            }
        }
    }
)
def delta_ingestion_job():
    """
    Incremental job running every 5 minutes
    Chain: extract -> enrich -> aggregate -> persist
    """
    runs = raw_mlflow_runs()
    enriched = enriched_metrics(runs)
    platform_agg = platform_aggregations(enriched)
    app_agg = application_aggregations(enriched)
    
    # Data quality checks
    validate_data_quality([enriched, platform_agg, app_agg])
    
    # Send notifications if critical metrics detected
    send_alerts_if_critical(platform_agg)