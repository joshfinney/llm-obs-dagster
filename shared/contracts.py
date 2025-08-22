from pydantic import BaseModel
from datetime import datetime
from typing import Dict, List, Optional, Literal
from enum import Enum

class MetricStatus(str, Enum):
    CRITICAL = "critical"
    WARNING = "warning"
    HEALTHY = "healthy"

class MetricMetadata(BaseModel):
    """Contract for Metrics API"""
    metric_name: str
    display_name: str
    description: str
    unit: Optional[str]
    category: str
    higher_is_better: bool = True

class ThresholdConfig(BaseModel):
    """Contract for Thresholds API"""
    metric_name: str
    critical_threshold: float
    warning_threshold: float
    experiment_id: Optional[str] = None  # None = platform default
    updated_at: datetime
    updated_by: str

class MLflowRun(BaseModel):
    run_id: str
    experiment_id: str
    experiment_name: str
    user_id: str
    start_time: datetime
    end_time: Optional[datetime]
    status: str
    metrics: Dict[str, float]
    params: Dict[str, str]
    tags: Dict[str, str]
    
class AggregatedMetric(BaseModel):
    """Pre-aggregated metric for efficient querying"""
    timestamp: datetime
    application: str
    metric_name: str
    value: float
    status: MetricStatus
    run_id: str
    user_id: str
    moving_avg: Optional[float]
    forecast: Optional[float]
    metadata: Dict