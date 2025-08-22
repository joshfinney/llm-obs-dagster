from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class MetricStatus(str, Enum):
    CRITICAL = "critical"
    WARNING = "warning"
    HEALTHY = "healthy"

class MetricMetadata(BaseModel):
    """Contract for Metrics API."""
    metric_name: str
    display_name: str
    description: str
    unit: str | None
    category: str
    higher_is_better: bool = True

class ThresholdConfig(BaseModel):
    """Contract for Thresholds API."""
    metric_name: str
    critical_threshold: float
    warning_threshold: float
    experiment_id: str | None = None  # None = platform default
    updated_at: datetime
    updated_by: str

class MLflowRun(BaseModel):
    run_id: str
    experiment_id: str
    experiment_name: str
    user_id: str
    start_time: datetime
    end_time: datetime | None
    status: str
    metrics: dict[str, float]
    params: dict[str, str]
    tags: dict[str, str]

class AggregatedMetric(BaseModel):
    """Pre-aggregated metric for efficient querying."""
    timestamp: datetime
    application: str
    metric_name: str
    value: float
    status: MetricStatus
    run_id: str
    user_id: str
    moving_avg: float | None
    forecast: float | None
    metadata: dict

class PlatformSummary(BaseModel):
    """Platform-level aggregated view."""
    timestamp: datetime
    total_runs: int
    critical_runs: int
    warning_runs: int
    healthy_runs: int
    pass_rate: float
    pass_rate_trend: float  # Change over last day
    recent_runs: list[str]  # Run IDs of 5 most recent
    applications: list[str]

class ApplicationSummary(BaseModel):
    """Application-level aggregated view."""
    timestamp: datetime
    application: str
    total_runs: int
    critical_runs: int
    warning_runs: int
    healthy_runs: int
    pass_rate: float
    metrics_summary: dict[str, dict]  # metric_name -> {latest_value, status, trend}

class TimeSeriesPoint(BaseModel):
    """Individual point for time series plots."""
    timestamp: datetime
    run_id: str
    run_name: str
    user_id: str
    metric_name: str
    value: float
    status: MetricStatus
    moving_avg: float | None
    forecast: float | None

class RunDetails(BaseModel):
    """Detailed run information for hover tooltips."""
    run_id: str
    run_name: str
    user_id: str
    timestamp: datetime
    application: str
    status: MetricStatus
    summary_metrics: dict[str, float]  # Key metrics for tooltip
