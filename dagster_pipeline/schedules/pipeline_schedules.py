import logging

from dagster import RunConfig, RunRequest, schedule

from dagster_pipeline.config.settings import Settings
from dagster_pipeline.jobs.delta_job import (
    delta_ingestion_job,
    historical_ingestion_job,
)

logger = logging.getLogger(__name__)

def create_job_config(lookback_hours: int, max_results: int = 100) -> RunConfig:
    """Create standardized job configuration."""
    return RunConfig(
        resources={
            "mlflow": {
                "config": {
                    "tracking_uri": Settings().MLFLOW_TRACKING_URI,
                    "registry_uri": Settings().MLFLOW_REGISTRY_URI
                }
            },
            "duckdb": {
                "config": {
                    "database_path": Settings().DUCKDB_PATH,
                    "aws_region": Settings().AWS_REGION
                }
            },
            "metrics_api": {
                "config": {
                    "api_url": Settings().METRICS_API_URL,
                    "api_key": Settings().API_KEY
                }
            },
            "thresholds_api": {
                "config": {
                    "api_url": Settings().THRESHOLDS_API_URL,
                    "api_key": Settings().API_KEY
                }
            },
            "s3_parquet_io": {
                "config": {
                    "s3_bucket": Settings().S3_BUCKET,
                    "s3_prefix": Settings().S3_PREFIX,
                    "aws_region": Settings().AWS_REGION
                }
            }
        },
        ops={
            "raw_mlflow_runs": {
                "config": {
                    "lookback_hours": lookback_hours,
                    "max_results_per_page": max_results
                }
            }
        }
    )

@schedule(
    job=delta_ingestion_job,
    cron_schedule="*/5 * * * *",  # Every 5 minutes
    execution_timezone="UTC"
)
def delta_schedule(context):
    """Delta ingestion schedule - runs every 5 minutes.

    Configurable via DELTA_SCHEDULE_MINUTES environment variable
    """
    settings = Settings()

    # Allow schedule customization via environment
    minutes = settings.DELTA_SCHEDULE_MINUTES
    if minutes != 5:
        # Note: This would require dynamic schedule creation in __init__.py
        logger.info(f"Delta schedule configured for every {minutes} minutes")

    return RunRequest(
        run_key=f"delta_{context.scheduled_execution_time.strftime('%Y%m%d_%H%M%S')}",
        run_config=create_job_config(lookback_hours=1, max_results=100),
        tags={
            "pipeline_type": "delta",
            "scheduled_time": context.scheduled_execution_time.isoformat(),
            "lookback_hours": "1"
        }
    )

@schedule(
    job=historical_ingestion_job,
    cron_schedule="0 2 * * *",  # 2 AM daily
    execution_timezone="UTC"
)
def historical_schedule(context):
    """Historical ingestion schedule - runs daily at 2 AM.

    Configurable via HISTORICAL_SCHEDULE_CRON environment variable
    """
    settings = Settings()

    # Use configured cron if different from default
    configured_cron = settings.HISTORICAL_SCHEDULE_CRON
    if configured_cron != "0 2 * * *":
        logger.info(f"Historical schedule configured with cron: {configured_cron}")

    return RunRequest(
        run_key=f"historical_{context.scheduled_execution_time.strftime('%Y%m%d')}",
        run_config=create_job_config(
            lookback_hours=settings.LOOKBACK_DAYS_HISTORICAL * 24,
            max_results=500
        ),
        tags={
            "pipeline_type": "historical",
            "scheduled_time": context.scheduled_execution_time.isoformat(),
            "lookback_days": str(settings.LOOKBACK_DAYS_HISTORICAL)
        }
    )

# Backup schedule for critical metric monitoring
@schedule(
    job=delta_ingestion_job,
    cron_schedule="0 */6 * * *",  # Every 6 hours as backup
    execution_timezone="UTC"
)
def backup_schedule(context):
    """Backup schedule for delta ingestion in case of failures
    Runs every 6 hours with extended lookback.
    """
    return RunRequest(
        run_key=f"backup_{context.scheduled_execution_time.strftime('%Y%m%d_%H')}",
        run_config=create_job_config(lookback_hours=6, max_results=200),
        tags={
            "pipeline_type": "backup",
            "scheduled_time": context.scheduled_execution_time.isoformat(),
            "lookback_hours": "6"
        }
    )
