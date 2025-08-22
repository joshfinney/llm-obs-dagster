from datetime import datetime
import logging

from dagster import AssetIn, Output, asset
import numpy as np
import pandas as pd

from shared.contracts import MetricMetadata, MetricStatus, ThresholdConfig

logger = logging.getLogger(__name__)

def calculate_metric_status(
    value: float,
    metric_metadata: MetricMetadata,
    threshold_config: ThresholdConfig
) -> MetricStatus:
    """Calculate metric status based on value and thresholds."""
    if metric_metadata.higher_is_better:
        # For metrics where higher is better (e.g., accuracy)
        if value < threshold_config.critical_threshold:
            return MetricStatus.CRITICAL
        elif value < threshold_config.warning_threshold:
            return MetricStatus.WARNING
        else:
            return MetricStatus.HEALTHY
    elif value > threshold_config.critical_threshold:
        return MetricStatus.CRITICAL
    elif value > threshold_config.warning_threshold:
        return MetricStatus.WARNING
    else:
        return MetricStatus.HEALTHY

def calculate_moving_average(
    df: pd.DataFrame,
    metric_name: str,
    window_hours: int = 24
) -> pd.Series:
    """Calculate moving average for a metric over time."""
    metric_data = df[df['metric_name'] == metric_name].copy()
    metric_data = metric_data.sort_values('start_time')

    # Use time-based rolling window
    metric_data = metric_data.set_index('start_time')
    return metric_data['metric_value'].rolling(
        window=f'{window_hours}H',
        min_periods=1
    ).mean()

@asset(
    ins={"raw_runs": AssetIn("raw_mlflow_runs")},
    required_resource_keys={"metrics_api", "thresholds_api", "settings"},
    io_manager_key="s3_parquet_io",
    compute_kind="enrichment",
    group_name="enrichment",
    description="Enrich raw metrics with metadata, thresholds, and calculated status"
)
def enriched_metrics(context, raw_runs: pd.DataFrame) -> pd.DataFrame:
    """Enrich metrics with metadata and thresholds from enterprise APIs.

    Steps:
    1. Extract unique metrics and experiments
    2. Batch fetch metadata from Metrics API
    3. Fetch threshold configurations (experiment-level overrides platform-level)
    4. Calculate metric status based on thresholds and metadata
    5. Add moving averages and forecasts
    6. Enrich with display metadata
    """
    if raw_runs.empty:
        context.log.warning("No raw runs to enrich")
        return pd.DataFrame()

    metrics_client = context.resources.metrics_api
    thresholds_client = context.resources.thresholds_api

    # Get unique metrics and experiments for batch processing
    unique_metrics = raw_runs['metric_name'].unique().tolist()
    unique_experiments = raw_runs['experiment_id'].unique().tolist()

    context.log.info(f"Enriching {len(unique_metrics)} metrics across {len(unique_experiments)} experiments")

    # Batch fetch metric metadata
    try:
        metadata_response = metrics_client.batch_get_metadata(unique_metrics)
        metrics_metadata = {m.metric_name: m for m in metadata_response}
        context.log.info(f"Fetched metadata for {len(metrics_metadata)} metrics")
    except Exception as e:
        context.log.error(f"Failed to fetch metrics metadata: {e!s}")
        raise

    # Batch fetch threshold configurations
    try:
        threshold_response = thresholds_client.get_thresholds(
            metrics=unique_metrics,
            experiments=unique_experiments
        )

        # Organize thresholds: experiment-level overrides platform-level
        thresholds_lookup = {}
        for threshold in threshold_response:
            key = (threshold.metric_name, threshold.experiment_id or "platform")
            thresholds_lookup[key] = threshold

        context.log.info(f"Fetched {len(threshold_response)} threshold configurations")
    except Exception as e:
        context.log.error(f"Failed to fetch thresholds: {e!s}")
        raise

    # Enrich each row with metadata and status
    enriched_data = []

    for _, row in raw_runs.iterrows():
        metric_name = row['metric_name']
        experiment_id = row['experiment_id']
        metric_value = row['metric_value']

        # Get metadata (required)
        metadata = metrics_metadata.get(metric_name)
        if not metadata:
            context.log.warning(f"No metadata found for metric: {metric_name}")
            continue

        # Get threshold config - experiment-level takes priority
        threshold_config = (
            thresholds_lookup.get((metric_name, experiment_id)) or
            thresholds_lookup.get((metric_name, "platform"))
        )

        if not threshold_config:
            context.log.warning(f"No threshold config found for metric: {metric_name}")
            continue

        # Calculate metric status
        status = calculate_metric_status(metric_value, metadata, threshold_config)

        # Create enriched record
        enriched_record = {
            # Original data
            **row.to_dict(),

            # Metadata enrichment
            'display_name': metadata.display_name,
            'description': metadata.description,
            'unit': metadata.unit,
            'category': metadata.category,
            'higher_is_better': metadata.higher_is_better,

            # Threshold enrichment
            'critical_threshold': threshold_config.critical_threshold,
            'warning_threshold': threshold_config.warning_threshold,
            'threshold_source': 'experiment' if threshold_config.experiment_id else 'platform',

            # Status calculation
            'status': status.value,

            # Processing metadata
            'enrichment_timestamp': datetime.now()
        }

        enriched_data.append(enriched_record)

    if not enriched_data:
        context.log.warning("No data remaining after enrichment")
        return pd.DataFrame()

    df = pd.DataFrame(enriched_data)

    # Calculate moving averages for each metric per experiment
    context.log.info("Calculating moving averages...")
    moving_averages = []

    for (experiment_id, metric_name), group in df.groupby(['experiment_id', 'metric_name']):
        try:
            group_sorted = group.sort_values('start_time')

            # Calculate 24-hour rolling average
            group_sorted['moving_avg_24h'] = group_sorted['metric_value'].rolling(
                window=min(len(group_sorted), 5),
                min_periods=1
            ).mean()

            # Simple forecast (next point based on trend)
            if len(group_sorted) >= 3:
                recent_values = group_sorted['metric_value'].tail(3).values
                trend = np.mean(np.diff(recent_values))
                group_sorted['forecast_next'] = group_sorted['metric_value'].iloc[-1] + trend
            else:
                group_sorted['forecast_next'] = group_sorted['metric_value']

            moving_averages.append(group_sorted)

        except Exception as e:
            context.log.warning(f"Error calculating moving average for {metric_name} in {experiment_id}: {e!s}")

    if moving_averages:
        df = pd.concat(moving_averages, ignore_index=True)

    # Log enrichment statistics
    status_counts = df['status'].value_counts().to_dict()

    stats = {
        "total_enriched_records": len(df),
        "unique_metrics": df['metric_name'].nunique(),
        "unique_experiments": df['experiment_id'].nunique(),
        "status_distribution": status_counts,
        "metrics_with_metadata": len([m for m in unique_metrics if m in metrics_metadata]),
        "avg_moving_avg": df['moving_avg_24h'].mean() if 'moving_avg_24h' in df.columns else None
    }

    context.log.info(f"Enrichment complete: {stats}")

    yield Output(
        df,
        metadata={
            **stats,
            "sample_critical_metrics": df[df['status'] == 'critical']['metric_name'].unique().tolist()[:5]
        }
    )
