# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Dagster-based data pipeline** for MLflow metrics monitoring and observability. The pipeline ingests MLflow experiment data, enriches it with metadata from enterprise APIs, performs aggregations, and provides real-time monitoring capabilities.

### Architecture

The pipeline follows a **layered asset-based architecture**:

1. **Data Ingestion** (`dagster_pipeline/assets/mlflow_ingestion.py`): Extracts runs from MLflow tracking server with time-based windows
2. **Data Enrichment** (`dagster_pipeline/assets/metric_enrichment.py`): Enriches metrics with metadata from enterprise APIs (Metrics API, Thresholds API) 
3. **Data Aggregation** (`dagster_pipeline/assets/aggregations.py`): Creates platform and application-level aggregations using DuckDB SQL
4. **Data Storage**: Persists to S3 in Parquet format using Dagster's S3 I/O manager

### Key Components

- **Jobs**: Two main execution modes
  - `delta_job.py`: 5-minute incremental ingestion (`delta_ingestion_job`)
  - Historical backfill job: Daily full refresh at 2 AM
- **Resources** (`dagster_pipeline/resources/`): MLflow client, DuckDB connection, S3 client, API clients
- **Schedules** (`dagster_pipeline/schedules/`): Cron-based scheduling for delta (every 5 min) and historical (daily) jobs
- **Configuration** (`dagster_pipeline/config/settings.py`): Pydantic-based settings with environment variable support
- **Contracts** (`shared/contracts.py`): Shared data models using Pydantic (MetricMetadata, ThresholdConfig, MLflowRun, AggregatedMetric)

### Data Flow

```
MLflow Tracking Server → Raw Runs → Enriched Metrics → Platform/App Aggregations → S3 Storage
                                         ↑
                                  Enterprise APIs
                                 (Metrics + Thresholds)
```

## Development Commands

This project uses **uv** for fast dependency management and **ruff** for linting/formatting.

### Initial Setup
```bash
# Install dependencies with uv
uv sync --dev

# Install pre-commit hooks
uv run pre-commit install

# Validate Dagster definitions
uv run dagster definitions validate
```

### Code Quality
```bash
# Lint and auto-fix with ruff
uv run ruff check . --fix

# Format code with ruff
uv run ruff format .

# Type checking with mypy
uv run mypy dagster_pipeline shared

# Spell checking
uv run cspell "**/*.py" "**/*.md" "**/*.toml" "**/*.yaml"

# Run all pre-commit hooks
uv run pre-commit run --all-files
```

### Running the Pipeline
```bash
# Start Dagster UI for development
uv run dagster dev --host 0.0.0.0 --port 3000

# Run specific job
uv run dagster job execute -j delta_ingestion_job

# Materialize specific assets
uv run dagster asset materialize --select raw_mlflow_runs

# Validate definitions
uv run dagster definitions validate
```

### Testing
```bash
# Run all tests
uv run pytest tests/ -v

# Run tests with coverage
uv run pytest tests/ --cov=dagster_pipeline --cov=shared --cov-report=html

# Run fast tests only (excluding slow/integration tests)
uv run pytest tests/ -m "not slow" -x --tb=short

# Run specific test file
uv run pytest tests/test_assets.py -v
```

### VS Code Integration
- Use `Ctrl+Shift+P` → "Tasks: Run Task" for common development tasks
- Debug configurations available for Dagster dev server, jobs, and assets
- Ruff formatting and linting integrated with save actions
- MyPy type checking enabled in editor

## Configuration Requirements

The pipeline requires these environment variables (see `dagster_pipeline/config/settings.py`):

- **MLflow**: `MLFLOW_TRACKING_URI`, `MLFLOW_REGISTRY_URI`
- **S3**: `S3_BUCKET`, `S3_PREFIX`, `AWS_REGION`  
- **APIs**: `METRICS_API_URL`, `THRESHOLDS_API_URL`, `API_KEY`
- **Database**: `DUCKDB_PATH`
- **Scheduling**: `DELTA_SCHEDULE_MINUTES`, `HISTORICAL_SCHEDULE_CRON`

## Key Libraries & Dependencies

- **Dagster**: Core orchestration framework (assets, jobs, resources, schedules)
- **MLflow**: Experiment tracking integration (`mlflow.client`)
- **DuckDB**: In-process analytical database for aggregations
- **Pandas**: Data manipulation and transformation
- **Pydantic**: Data validation and settings management
- **AWS S3**: Data storage backend

## Development Patterns

- **Asset-based development**: New data transformations should be implemented as Dagster assets
- **Resource dependency**: Assets declare required resources via `required_resource_keys`
- **Configuration**: Use Pydantic settings in `config/settings.py` for new configuration
- **Data contracts**: Define new data models in `shared/contracts.py`
- **SQL-heavy aggregations**: Use DuckDB for complex analytical queries in aggregation assets
- **Time-based processing**: Pipeline supports both delta (incremental) and historical (full) processing modes