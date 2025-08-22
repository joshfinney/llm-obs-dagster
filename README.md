# MLflow Metrics Monitoring Pipeline

A comprehensive, production-ready Dagster pipeline for real-time MLflow metrics monitoring and observability.

## Overview

This pipeline provides:
- **Real-time monitoring** of MLflow experiments with 5-minute delta processing
- **Traffic light dashboards** showing platform and application-level health 
- **Time series visualizations** with forecasting and threshold alerting
- **Enterprise integration** with configurable metrics metadata and thresholds APIs
- **Scalable S3 persistence** optimized for Streamlit frontend consumption

## Architecture

```
MLflow Tracking Server → Raw Runs → Enriched Metrics → Aggregations → S3 Storage
                                         ↑                    ↓
                              Enterprise APIs        Streamlit Dashboard
                             (Metrics + Thresholds)
```

### Key Components

1. **Data Ingestion** (`dagster_pipeline/assets/mlflow_ingestion.py`)
   - Extracts runs from MLflow with configurable time windows
   - Handles pagination and error recovery
   - Flattens metrics for efficient processing

2. **Enrichment** (`dagster_pipeline/assets/metric_enrichment.py`) 
   - Fetches metadata from enterprise Metrics API
   - Applies threshold configurations (experiment-level overrides platform-level)
   - Calculates metric status (critical/warning/healthy)
   - Adds moving averages and forecasting

3. **Aggregations** (`dagster_pipeline/assets/aggregations.py`)
   - **Platform Summary**: Overall traffic lights, pass rates, trends
   - **Application Summary**: Per-application metrics and status
   - **Time Series**: Plot-ready data with hover details

4. **Resources** (`dagster_pipeline/resources/`)
   - **MLflow Client**: Configured MLflow tracking client
   - **Enterprise APIs**: Retryable HTTP clients for metadata/thresholds
   - **DuckDB**: High-performance analytical queries
   - **S3 I/O Manager**: Optimized Parquet storage

5. **Jobs & Scheduling** (`dagster_pipeline/jobs/`, `dagster_pipeline/schedules/`)
   - **Delta Job**: Every 5 minutes (1-hour lookback)
   - **Historical Job**: Daily at 2 AM (30-day lookback)
   - **Backup Job**: Every 6 hours (safety net)

## Quick Start

### 1. Install Dependencies
```bash
# Install with uv (recommended)
uv sync --dev

# Install pre-commit hooks
uv run pre-commit install
```

### 2. Configure Environment
```bash
# Copy and edit environment variables
cp .env.example .env

# Required variables:
export MLFLOW_TRACKING_URI="http://localhost:5000"
export S3_BUCKET="your-metrics-bucket"
export METRICS_API_URL="https://api.company.com/metrics"
export THRESHOLDS_API_URL="https://api.company.com/thresholds" 
export API_KEY="your-api-key"
```

### 3. Validate Configuration
```bash
# Validate Dagster definitions
uv run dagster definitions validate

# Run data quality checks
uv run dagster asset materialize --select raw_mlflow_runs
```

### 4. Start Development Server
```bash
# Launch Dagster UI
uv run dagster dev --host 0.0.0.0 --port 3000

# Access at http://localhost:3000
```

## Configuration

### Enterprise API Contracts

**Metrics API** (`/metrics/batch`):
```json
{
  "metric_names": ["accuracy", "f1_score", "latency"],
  "response": {
    "metrics": [
      {
        "metric_name": "accuracy",
        "display_name": "Model Accuracy",
        "description": "Classification accuracy percentage",
        "unit": "%",
        "category": "quality",
        "higher_is_better": true
      }
    ]
  }
}
```

**Thresholds API** (`/thresholds/batch`):
```json
{
  "metrics": ["accuracy"],
  "experiments": ["exp-123"],
  "response": {
    "thresholds": [
      {
        "metric_name": "accuracy",
        "critical_threshold": 0.8,
        "warning_threshold": 0.85,
        "experiment_id": null,
        "updated_at": "2025-01-15T10:00:00Z",
        "updated_by": "platform-admin"
      }
    ]
  }
}
```

### Scheduling Configuration

```python
# Environment variables for scheduling
DELTA_SCHEDULE_MINUTES=5          # Default: every 5 minutes
HISTORICAL_SCHEDULE_CRON="0 2 * * *"  # Default: 2 AM daily
LOOKBACK_DAYS_HISTORICAL=30       # Default: 30 days
```

## Data Flow

### Frontend Data Structure

The pipeline generates optimized datasets for Streamlit consumption:

**Platform Summary** (S3: `platform_summary/YYYY/MM/DD/`):
```python
{
  'timestamp': datetime,
  'total_runs': int,
  'critical_runs': int, 
  'warning_runs': int,
  'healthy_runs': int,
  'pass_rate': float,
  'pass_rate_trend': float,
  'recent_run_ids': List[str],
  'active_applications': List[str]
}
```

**Application Summary** (S3: `application_summary/YYYY/MM/DD/`):
```python
{
  'timestamp': datetime,
  'application': str,
  'total_runs': int,
  'critical_runs': int,
  'warning_runs': int, 
  'healthy_runs': int,
  'pass_rate': float,
  'metrics_summary': {
    'metric_name': {
      'latest_value': float,
      'latest_status': str,
      'avg_value': float,
      'trend': float,
      'display_name': str,
      'unit': str
    }
  }
}
```

**Time Series** (S3: `metric_timeseries/YYYY/MM/DD/`):
```python
{
  'timestamp': datetime,
  'run_id': str,
  'run_name': str,
  'user_id': str,
  'application': str,
  'metric_name': str,
  'value': float,
  'status': str,
  'moving_avg': float,
  'forecast': float,
  'critical_threshold': float,
  'warning_threshold': float,
  'run_summary_metrics': dict  # For hover tooltips
}
```

## Monitoring & Observability

### Logging
- **Structured JSON logging** for production environments
- **Rich console output** for development
- **Performance metrics** tracking
- **Error aggregation** with context

### Data Quality
- **Null value detection**
- **Duplicate record identification** 
- **Data freshness validation**
- **Metric range verification**
- **Schema compliance checking**

### Alerting
- **Critical metric thresholds** (>20% critical runs)
- **Pass rate degradation** (<80% pass rate)
- **Data quality failures**
- **API connection issues**

## Development

### Running Tests
```bash
# Run all tests
uv run pytest tests/ -v

# Run with coverage
uv run pytest tests/ --cov=dagster_pipeline --cov=shared

# Run fast tests only
uv run pytest tests/ -m "not slow"
```

### Code Quality
```bash
# Lint and format
uv run ruff check . --fix
uv run ruff format .

# Type checking  
uv run mypy dagster_pipeline shared

# Run all pre-commit hooks
uv run pre-commit run --all-files
```

### Manual Execution
```bash
# Run specific job
uv run dagster job execute -j delta_ingestion_job

# Materialize specific assets
uv run dagster asset materialize --select enriched_metrics

# Backfill historical data
uv run dagster job execute -j historical_ingestion_job
```

## Production Deployment

### Infrastructure Requirements
- **Compute**: 4 CPU, 8GB RAM minimum
- **Storage**: S3 bucket with lifecycle policies
- **Database**: DuckDB for analytical queries
- **Networking**: Access to MLflow tracking server and enterprise APIs

### Scaling Considerations
- **Horizontal scaling**: Multiple Dagster instances with job partitioning
- **Data partitioning**: S3 partitions by date/application
- **Caching**: API response caching for metadata/thresholds
- **Monitoring**: CloudWatch/Prometheus integration

### Security
- **API authentication**: Bearer token authentication
- **S3 access**: IAM roles with minimal permissions
- **Data encryption**: In-transit and at-rest encryption
- **Audit logging**: Comprehensive audit trails

## Troubleshooting

### Common Issues

**High memory usage**: Increase DuckDB memory limits or reduce batch sizes
```python
BATCH_SIZE=50  # Reduce from default 100
```

**API rate limiting**: Adjust retry configuration
```python
max_retries=5
timeout=60
```

**S3 write failures**: Check IAM permissions and bucket policies

**MLflow connection issues**: Verify tracking URI and network connectivity

### Performance Optimization

1. **Batch size tuning**: Adjust `max_results_per_page` based on data volume
2. **Lookback optimization**: Reduce lookback windows for faster processing  
3. **S3 partitioning**: Optimize partition strategy for query patterns
4. **DuckDB optimization**: Tune memory and thread settings

## Contributing

1. **Fork** the repository
2. **Create** a feature branch
3. **Add tests** for new functionality
4. **Run** pre-commit hooks
5. **Submit** a pull request

## License

MIT License - see LICENSE file for details.

---

For questions or support, please open an issue or contact the data platform team.