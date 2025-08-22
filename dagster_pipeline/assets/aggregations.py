import logging

from dagster import AssetIn, Output, asset
import pandas as pd

logger = logging.getLogger(__name__)

@asset(
    ins={"enriched": AssetIn("enriched_metrics")},
    required_resource_keys={"duckdb"},
    io_manager_key="s3_parquet_io",
    compute_kind="duckdb",
    group_name="aggregations",
    description="Platform-level aggregations for traffic light dashboard"
)
def platform_summary(context, enriched: pd.DataFrame) -> pd.DataFrame:
    """Create platform-level aggregations for main dashboard.

    Produces:
    - Overall traffic light status
    - Pass rate trends
    - Recent runs summary
    - Top failing metrics
    """
    if enriched.empty:
        context.log.warning("No enriched data for platform aggregations")
        return pd.DataFrame()

    duckdb_conn = context.resources.duckdb

    # Register enriched DataFrame as a DuckDB table
    duckdb_conn.register('enriched_metrics', enriched)

    context.log.info("Creating platform-level aggregations...")

    platform_query = """
    WITH run_status AS (
        -- Determine status per run (critical if ANY metric is critical)
        SELECT
            run_id,
            experiment_name,
            user_id,
            start_time,
            CASE
                WHEN COUNT(CASE WHEN status = 'critical' THEN 1 END) > 0 THEN 'critical'
                WHEN COUNT(CASE WHEN status = 'warning' THEN 1 END) > 0 THEN 'warning'
                ELSE 'healthy'
            END AS run_status,
            COUNT(*) as total_metrics,
            COUNT(CASE WHEN status = 'critical' THEN 1 END) as critical_metrics,
            COUNT(CASE WHEN status = 'warning' THEN 1 END) as warning_metrics
        FROM enriched_metrics
        GROUP BY run_id, experiment_name, user_id, start_time
    ),
    platform_stats AS (
        SELECT
            DATE_TRUNC('hour', start_time) as time_hour,
            COUNT(*) as total_runs,
            COUNT(CASE WHEN run_status = 'critical' THEN 1 END) as critical_runs,
            COUNT(CASE WHEN run_status = 'warning' THEN 1 END) as warning_runs,
            COUNT(CASE WHEN run_status = 'healthy' THEN 1 END) as healthy_runs,

            -- Pass rate (healthy + warning runs / total runs)
            (COUNT(CASE WHEN run_status IN ('healthy', 'warning') THEN 1 END) * 100.0 /
             COUNT(*)) as pass_rate,

            -- Recent runs (get run IDs for latest runs)
            ARRAY_AGG(run_id ORDER BY start_time DESC)[1:5] as recent_run_ids,
            ARRAY_AGG(DISTINCT experiment_name) as active_applications
        FROM run_status
        GROUP BY DATE_TRUNC('hour', start_time)
    ),
    trending AS (
        SELECT *,
            LAG(pass_rate, 24) OVER (ORDER BY time_hour) as pass_rate_24h_ago,
            pass_rate - LAG(pass_rate, 24) OVER (ORDER BY time_hour) as pass_rate_trend
        FROM platform_stats
    )
    SELECT
        time_hour as timestamp,
        total_runs,
        critical_runs,
        warning_runs,
        healthy_runs,
        pass_rate,
        COALESCE(pass_rate_trend, 0) as pass_rate_trend,
        recent_run_ids,
        active_applications,
        CURRENT_TIMESTAMP as aggregation_timestamp
    FROM trending
    ORDER BY time_hour DESC
    """

    try:
        result = duckdb_conn.execute(platform_query).df()
        context.log.info(f"Generated {len(result)} hourly platform summaries")

        # Log key metrics
        if len(result) > 0:
            latest = result.iloc[0]
            context.log.info(
                f"Latest platform status - Total: {latest['total_runs']}, "
                f"Critical: {latest['critical_runs']}, "
                f"Pass Rate: {latest['pass_rate']:.1f}%"
            )

        yield Output(
            result,
            metadata={
                "records_generated": len(result),
                "latest_pass_rate": (
                    float(result.iloc[0]['pass_rate']) if len(result) > 0 else None
                ),
                "latest_critical_runs": (
                    int(result.iloc[0]['critical_runs']) if len(result) > 0 else 0
                )
            }
        )

    except Exception as e:
        context.log.error(f"Error creating platform aggregations: {e!s}")
        raise

@asset(
    ins={"enriched": AssetIn("enriched_metrics")},
    required_resource_keys={"duckdb"},
    io_manager_key="s3_parquet_io",
    compute_kind="duckdb",
    group_name="aggregations",
    description="Application-level aggregations for drill-down views"
)
def application_summary(context, enriched: pd.DataFrame) -> pd.DataFrame:
    """Create application-level aggregations for detailed views.

    Produces per-application:
    - Traffic light status
    - Metrics summary with trends
    - Time series data points
    """
    if enriched.empty:
        context.log.warning("No enriched data for application aggregations")
        return pd.DataFrame()

    duckdb_conn = context.resources.duckdb
    duckdb_conn.register('enriched_metrics', enriched)

    context.log.info("Creating application-level aggregations...")

    app_query = """
    WITH app_run_status AS (
        -- Determine status per run per application
        SELECT
            experiment_name as application,
            run_id,
            user_id,
            start_time,
            CASE
                WHEN COUNT(CASE WHEN status = 'critical' THEN 1 END) > 0 THEN 'critical'
                WHEN COUNT(CASE WHEN status = 'warning' THEN 1 END) > 0 THEN 'warning'
                ELSE 'healthy'
            END AS run_status
        FROM enriched_metrics
        GROUP BY experiment_name, run_id, user_id, start_time
    ),
    app_metrics_summary AS (
        -- Latest status and trend for each metric per application
        SELECT
            experiment_name as application,
            metric_name,
            display_name,
            category,
            unit,
            COUNT(*) as total_measurements,
            AVG(metric_value) as avg_value,
            STDDEV(metric_value) as stddev_value,
            MIN(metric_value) as min_value,
            MAX(metric_value) as max_value,

            -- Latest measurement
            FIRST_VALUE(metric_value ORDER BY start_time DESC) as latest_value,
            FIRST_VALUE(status ORDER BY start_time DESC) as latest_status,
            FIRST_VALUE(moving_avg_24h ORDER BY start_time DESC) as latest_moving_avg,

            -- Trend calculation
            CORR(EXTRACT(EPOCH FROM start_time), metric_value) as trend_correlation
        FROM enriched_metrics
        WHERE start_time >= NOW() - INTERVAL '7 days'  -- Last week for trends
        GROUP BY experiment_name, metric_name, display_name, category, unit
    ),
    app_summary AS (
        SELECT
            application,
            DATE_TRUNC('hour', start_time) as time_hour,
            COUNT(*) as total_runs,
            COUNT(CASE WHEN run_status = 'critical' THEN 1 END) as critical_runs,
            COUNT(CASE WHEN run_status = 'warning' THEN 1 END) as warning_runs,
            COUNT(CASE WHEN run_status = 'healthy' THEN 1 END) as healthy_runs,
            (COUNT(CASE WHEN run_status IN ('healthy', 'warning') THEN 1 END) * 100.0 /
             COUNT(*)) as pass_rate
        FROM app_run_status
        GROUP BY application, DATE_TRUNC('hour', start_time)
    )
    SELECT
        s.time_hour as timestamp,
        s.application,
        s.total_runs,
        s.critical_runs,
        s.warning_runs,
        s.healthy_runs,
        s.pass_rate,
        -- Aggregate metrics summary as JSON
        OBJECT_AGG(
            ms.metric_name,
            {
                'latest_value': ms.latest_value,
                'latest_status': ms.latest_status,
                'avg_value': ms.avg_value,
                'trend': ms.trend_correlation,
                'display_name': ms.display_name,
                'unit': ms.unit
            }
        ) as metrics_summary,
        CURRENT_TIMESTAMP as aggregation_timestamp
    FROM app_summary s
    LEFT JOIN app_metrics_summary ms ON s.application = ms.application
    GROUP BY s.time_hour, s.application, s.total_runs, s.critical_runs,
             s.warning_runs, s.healthy_runs, s.pass_rate
    ORDER BY s.application, s.time_hour DESC
    """

    try:
        result = duckdb_conn.execute(app_query).df()
        context.log.info(f"Generated {len(result)} application summaries")

        # Log summary by application
        if len(result) > 0:
            apps = result['application'].unique()
            context.log.info(
                f"Generated summaries for {len(apps)} applications: {list(apps)}"
            )

        yield Output(
            result,
            metadata={
                "records_generated": len(result),
                "applications_covered": (
                    result['application'].nunique() if len(result) > 0 else 0
                ),
                "avg_pass_rate": (
                    float(result['pass_rate'].mean()) if len(result) > 0 else None
                )
            }
        )

    except Exception as e:
        context.log.error(f"Error creating application aggregations: {e!s}")
        raise

@asset(
    ins={"enriched": AssetIn("enriched_metrics")},
    required_resource_keys={"duckdb"},
    io_manager_key="s3_parquet_io",
    compute_kind="duckdb",
    group_name="aggregations",
    description="Time series data for metric plots with hover details"
)
def metric_timeseries(context, enriched: pd.DataFrame) -> pd.DataFrame:
    """Create time series points for frontend metric plots.

    Each row represents a point on a time series plot with:
    - Timestamp, value, status
    - Moving average and forecast
    - Hover tooltip information
    """
    if enriched.empty:
        context.log.warning("No enriched data for timeseries")
        return pd.DataFrame()

    duckdb_conn = context.resources.duckdb
    duckdb_conn.register('enriched_metrics', enriched)

    context.log.info("Creating metric time series data...")

    timeseries_query = """
    SELECT
        start_time as timestamp,
        run_id,
        run_name,
        user_id,
        experiment_name as application,
        metric_name,
        display_name,
        metric_value as value,
        status,
        moving_avg_24h as moving_avg,
        forecast_next as forecast,
        critical_threshold,
        warning_threshold,
        unit,
        description,
        -- Additional context for hover tooltips
        OBJECT_AGG(
            param_key, param_value
        ) FILTER (
            WHERE param_key IN ('model_type', 'dataset', 'version')
        ) as key_params,

        -- Summary metrics for this run (for tooltip)
        (
            SELECT OBJECT_AGG(metric_name, metric_value)
            FROM enriched_metrics e2
            WHERE e2.run_id = e1.run_id
        ) as run_summary_metrics,

        extraction_timestamp,
        enrichment_timestamp
    FROM enriched_metrics e1,
    UNNEST(
        ARRAY[
            ['model_type', params['model_type']],
            ['dataset', params['dataset']],
            ['version', params['version']]
        ]
    ) AS t(param_key, param_value)
    WHERE start_time >= NOW() - INTERVAL '30 days'  -- Last 30 days for charts
    GROUP BY
        start_time, run_id, run_name, user_id, experiment_name, metric_name,
        display_name, metric_value, status, moving_avg_24h, forecast_next,
        critical_threshold, warning_threshold, unit, description,
        extraction_timestamp, enrichment_timestamp
    ORDER BY experiment_name, metric_name, start_time
    """

    try:
        result = duckdb_conn.execute(timeseries_query).df()
        context.log.info(f"Generated {len(result)} time series points")

        # Log coverage
        if len(result) > 0:
            apps = result['application'].nunique()
            metrics = result['metric_name'].nunique()
            date_range = f"{result['timestamp'].min()} to {result['timestamp'].max()}"
            context.log.info(
                f"Time series covers {apps} apps, {metrics} metrics from {date_range}"
            )

        yield Output(
            result,
            metadata={
                "timeseries_points": len(result),
                "applications": result['application'].nunique() if len(result) > 0 else 0,
                "metrics": result['metric_name'].nunique() if len(result) > 0 else 0,
                "date_range_start": (
                    result['timestamp'].min().isoformat() if len(result) > 0 else None
                ),
                "date_range_end": (
                    result['timestamp'].max().isoformat() if len(result) > 0 else None
                )
            }
        )

    except Exception as e:
        context.log.error(f"Error creating timeseries data: {e!s}")
        raise
