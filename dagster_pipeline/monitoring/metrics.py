from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import logging
import time
from typing import Any

from dagster import Out, op
import pandas as pd

logger = logging.getLogger(__name__)

class AlertSeverity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class DataQualityCheck:
    """Data quality check definition."""
    name: str
    description: str
    check_function: callable
    severity: AlertSeverity
    enabled: bool = True

@dataclass
class PipelineMetric:
    """Pipeline execution metric."""
    name: str
    value: float
    unit: str
    timestamp: datetime
    tags: dict[str, str]

@dataclass
class Alert:
    """Pipeline alert."""
    title: str
    message: str
    severity: AlertSeverity
    timestamp: datetime
    context: dict[str, Any]
    resolved: bool = False

class DataQualityValidator:
    """Comprehensive data quality validation."""

    def __init__(self):
        self.checks = [
            DataQualityCheck(
                name="null_values",
                description="Check for excessive null values",
                check_function=self._check_null_values,
                severity=AlertSeverity.MEDIUM
            ),
            DataQualityCheck(
                name="duplicate_records",
                description="Check for duplicate records",
                check_function=self._check_duplicates,
                severity=AlertSeverity.LOW
            ),
            DataQualityCheck(
                name="data_freshness",
                description="Check data freshness",
                check_function=self._check_freshness,
                severity=AlertSeverity.HIGH
            ),
            DataQualityCheck(
                name="metric_ranges",
                description="Check metric values are in expected ranges",
                check_function=self._check_metric_ranges,
                severity=AlertSeverity.MEDIUM
            ),
            DataQualityCheck(
                name="schema_validation",
                description="Validate expected columns exist",
                check_function=self._check_schema,
                severity=AlertSeverity.HIGH
            )
        ]

    def _check_null_values(self, df: pd.DataFrame) -> tuple[bool, dict[str, Any]]:
        """Check for excessive null values."""
        null_percentages = (df.isnull().sum() / len(df)) * 100
        excessive_nulls = null_percentages[null_percentages > 10]  # > 10% nulls

        return len(excessive_nulls) == 0, {
            "null_percentages": null_percentages.to_dict(),
            "excessive_null_columns": list(excessive_nulls.index)
        }

    def _check_duplicates(self, df: pd.DataFrame) -> tuple[bool, dict[str, Any]]:
        """Check for duplicate records."""
        if 'run_id' in df.columns and 'metric_name' in df.columns:
            duplicates = df.duplicated(subset=['run_id', 'metric_name']).sum()
            duplicate_rate = (duplicates / len(df)) * 100

            return duplicate_rate < 1, {  # < 1% duplicates allowed
                "duplicate_count": duplicates,
                "duplicate_rate": duplicate_rate
            }

        return True, {"message": "No unique identifier columns found"}

    def _check_freshness(self, df: pd.DataFrame) -> tuple[bool, dict[str, Any]]:
        """Check data freshness."""
        if 'start_time' in df.columns:
            latest_time = pd.to_datetime(df['start_time']).max()
            age_hours = (datetime.now() - latest_time).total_seconds() / 3600

            return age_hours < 24, {  # Data should be < 24 hours old
                "latest_timestamp": latest_time.isoformat(),
                "age_hours": age_hours
            }

        return True, {"message": "No timestamp column found"}

    def _check_metric_ranges(self, df: pd.DataFrame) -> tuple[bool, dict[str, Any]]:
        """Check metric values are reasonable."""
        if 'metric_value' in df.columns:
            values = df['metric_value'].dropna()

            # Check for extreme outliers (beyond 3 standard deviations)
            mean_val = values.mean()
            std_val = values.std()
            outliers = values[(values < mean_val - 3*std_val) | (values > mean_val + 3*std_val)]
            outlier_rate = len(outliers) / len(values) * 100

            return outlier_rate < 5, {  # < 5% outliers allowed
                "outlier_count": len(outliers),
                "outlier_rate": outlier_rate,
                "value_stats": {
                    "mean": mean_val,
                    "std": std_val,
                    "min": values.min(),
                    "max": values.max()
                }
            }

        return True, {"message": "No metric_value column found"}

    def _check_schema(self, df: pd.DataFrame) -> tuple[bool, dict[str, Any]]:
        """Validate expected schema."""
        required_columns = ['run_id', 'experiment_name', 'metric_name', 'metric_value', 'start_time']
        missing_columns = [col for col in required_columns if col not in df.columns]

        return len(missing_columns) == 0, {
            "required_columns": required_columns,
            "missing_columns": missing_columns,
            "actual_columns": list(df.columns)
        }

    def validate_dataframe(self, df: pd.DataFrame, dataset_name: str) -> list[Alert]:
        """Run all data quality checks on a DataFrame."""
        alerts = []

        logger.info(f"Running data quality checks on {dataset_name}")

        for check in self.checks:
            if not check.enabled:
                continue

            try:
                start_time = time.time()
                passed, details = check.check_function(df)
                duration = (time.time() - start_time) * 1000

                logger.info(f"✅ Check '{check.name}': {'PASSED' if passed else 'FAILED'} ({duration:.2f}ms)")

                if not passed:
                    alert = Alert(
                        title=f"Data Quality Issue: {check.name}",
                        message=f"{check.description} failed for {dataset_name}",
                        severity=check.severity,
                        timestamp=datetime.now(),
                        context={
                            "dataset": dataset_name,
                            "check_name": check.name,
                            "details": details,
                            "record_count": len(df)
                        }
                    )
                    alerts.append(alert)

            except Exception as e:
                logger.error(f"Error running check '{check.name}': {e!s}")
                alert = Alert(
                    title=f"Data Quality Check Error: {check.name}",
                    message=f"Error running {check.name} on {dataset_name}: {e!s}",
                    severity=AlertSeverity.HIGH,
                    timestamp=datetime.now(),
                    context={
                        "dataset": dataset_name,
                        "check_name": check.name,
                        "error": str(e)
                    }
                )
                alerts.append(alert)

        return alerts

class MetricsCollector:
    """Collect and track pipeline execution metrics."""

    def __init__(self):
        self.metrics: list[PipelineMetric] = []

    def record_metric(self, name: str, value: float, unit: str = "", tags: dict[str, str] | None = None):
        """Record a pipeline metric."""
        metric = PipelineMetric(
            name=name,
            value=value,
            unit=unit,
            timestamp=datetime.now(),
            tags=tags or {}
        )
        self.metrics.append(metric)

        logger.info(f"📊 Metric: {name} = {value} {unit}", extra={
            "metric_name": name,
            "metric_value": value,
            "metric_unit": unit,
            "metric_tags": tags,
            "event_type": "metric"
        })

    def record_execution_time(self, operation: str, duration_ms: float, tags: dict[str, str] | None = None):
        """Record execution time metric."""
        self.record_metric(
            name=f"execution_time_{operation}",
            value=duration_ms,
            unit="ms",
            tags=tags
        )

    def record_data_volume(self, dataset: str, record_count: int, size_mb: float | None = None):
        """Record data volume metrics."""
        self.record_metric(
            name=f"record_count_{dataset}",
            value=record_count,
            unit="records",
            tags={"dataset": dataset}
        )

        if size_mb is not None:
            self.record_metric(
                name=f"data_size_{dataset}",
                value=size_mb,
                unit="MB",
                tags={"dataset": dataset}
            )

    def record_status_distribution(self, status_counts: dict[str, int]):
        """Record metric status distribution."""
        total = sum(status_counts.values())

        for status, count in status_counts.items():
            percentage = (count / total) * 100 if total > 0 else 0
            self.record_metric(
                name=f"status_percentage_{status}",
                value=percentage,
                unit="%",
                tags={"status": status}
            )

    def get_metrics_summary(self) -> dict[str, Any]:
        """Get summary of collected metrics."""
        if not self.metrics:
            return {}

        return {
            "total_metrics": len(self.metrics),
            "time_range": {
                "start": min(m.timestamp for m in self.metrics).isoformat(),
                "end": max(m.timestamp for m in self.metrics).isoformat()
            },
            "metric_types": list({m.name for m in self.metrics}),
            "latest_metrics": [
                {
                    "name": m.name,
                    "value": m.value,
                    "unit": m.unit,
                    "timestamp": m.timestamp.isoformat()
                }
                for m in sorted(self.metrics, key=lambda x: x.timestamp, reverse=True)[:10]
            ]
        }

@op(
    out=Out(dict[str, Any], description="Data quality validation results"),
    description="Validate data quality and generate alerts"
)
def validate_data_quality(context, datasets: list[pd.DataFrame], dataset_names: list[str]) -> dict[str, Any]:
    """Op to validate data quality across multiple datasets."""
    validator = DataQualityValidator()
    all_alerts = []

    for df, name in zip(datasets, dataset_names, strict=False):
        alerts = validator.validate_dataframe(df, name)
        all_alerts.extend(alerts)

    # Log summary
    critical_alerts = [a for a in all_alerts if a.severity == AlertSeverity.CRITICAL]
    high_alerts = [a for a in all_alerts if a.severity == AlertSeverity.HIGH]

    context.log.info(f"Data quality validation complete: {len(all_alerts)} total alerts")
    context.log.info(f"Critical: {len(critical_alerts)}, High: {len(high_alerts)}")

    if critical_alerts:
        context.log.error(f"🚨 {len(critical_alerts)} CRITICAL data quality issues detected!")
        for alert in critical_alerts:
            context.log.error(f"  - {alert.title}: {alert.message}")

    return {
        "validation_timestamp": datetime.now().isoformat(),
        "total_alerts": len(all_alerts),
        "alerts_by_severity": {
            severity.value: len([a for a in all_alerts if a.severity == severity])
            for severity in AlertSeverity
        },
        "alerts": [
            {
                "title": alert.title,
                "message": alert.message,
                "severity": alert.severity.value,
                "timestamp": alert.timestamp.isoformat(),
                "context": alert.context
            }
            for alert in all_alerts
        ]
    }

@op(
    description="Check for critical metrics and send alerts"
)
def check_critical_metrics(context, platform_summary: pd.DataFrame) -> dict[str, Any]:
    """Check for critical metrics and generate alerts."""
    if platform_summary.empty:
        return {"status": "no_data", "alerts": []}

    latest_summary = platform_summary.iloc[0]
    critical_runs = latest_summary.get('critical_runs', 0)
    total_runs = latest_summary.get('total_runs', 0)
    pass_rate = latest_summary.get('pass_rate', 100)

    alerts = []

    # Check critical run threshold
    if total_runs > 0:
        critical_rate = (critical_runs / total_runs) * 100

        if critical_rate > 20:  # > 20% critical
            alerts.append({
                "title": "High Critical Run Rate",
                "message": f"{critical_rate:.1f}% of runs are critical ({critical_runs}/{total_runs})",
                "severity": AlertSeverity.CRITICAL.value,
                "timestamp": datetime.now().isoformat()
            })
        elif critical_rate > 10:  # > 10% critical
            alerts.append({
                "title": "Elevated Critical Run Rate",
                "message": f"{critical_rate:.1f}% of runs are critical ({critical_runs}/{total_runs})",
                "severity": AlertSeverity.HIGH.value,
                "timestamp": datetime.now().isoformat()
            })

    # Check pass rate threshold
    if pass_rate < 80:  # < 80% pass rate
        alerts.append({
            "title": "Low Pass Rate",
            "message": f"Pass rate dropped to {pass_rate:.1f}%",
            "severity": AlertSeverity.HIGH.value,
            "timestamp": datetime.now().isoformat()
        })

    # Log alerts
    if alerts:
        context.log.warning(f"🚨 Generated {len(alerts)} critical metric alerts")
        for alert in alerts:
            context.log.warning(f"  - {alert['title']}: {alert['message']}")
    else:
        context.log.info("✅ No critical metric alerts")

    return {
        "check_timestamp": datetime.now().isoformat(),
        "critical_runs": critical_runs,
        "total_runs": total_runs,
        "pass_rate": pass_rate,
        "alerts": alerts
    }
