
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # MLflow Configuration
    MLFLOW_TRACKING_URI: str
    MLFLOW_REGISTRY_URI: str | None

    # S3 Configuration
    S3_BUCKET: str
    S3_PREFIX: str = "mlflow-metrics"
    AWS_REGION: str = "us-east-1"

    # DuckDB Configuration
    DUCKDB_PATH: str = "/tmp/metrics.duckdb"

    # Enterprise API Configuration
    METRICS_API_URL: str
    THRESHOLDS_API_URL: str
    API_KEY: str

    # Schedule Configuration
    DELTA_SCHEDULE_MINUTES: int = 5
    HISTORICAL_SCHEDULE_CRON: str = "0 2 * * *"  # 2 AM daily

    # Processing Configuration
    LOOKBACK_DAYS_DELTA: int = 1
    LOOKBACK_DAYS_HISTORICAL: int = 30
    BATCH_SIZE: int = 100

    class Config:
        env_file = ".env"
