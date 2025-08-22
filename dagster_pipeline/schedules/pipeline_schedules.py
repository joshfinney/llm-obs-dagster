from dagster import schedule, ScheduleDefinition
from croniter import croniter

delta_schedule = ScheduleDefinition(
    job=delta_ingestion_job,
    cron_schedule="*/5 * * * *",  # Every 5 minutes
    execution_timezone="UTC"
)

historical_schedule = ScheduleDefinition(
    job=historical_backfill_job,
    cron_schedule="0 2 * * *",  # 2 AM daily
    execution_timezone="UTC"
)