mlflow-metrics-monitor/
├── dagster_pipeline/
│   ├── __init__.py
│   ├── config/
│   │   ├── __init__.py
│   │   ├── settings.py              # Pydantic settings management
│   │   └── schemas.py               # Data schemas/contracts
│   ├── assets/
│   │   ├── __init__.py
│   │   ├── mlflow_ingestion.py     # MLflow data extraction
│   │   ├── metric_enrichment.py    # Enrich with API metadata
│   │   ├── aggregations.py         # Pre-aggregation logic
│   │   └── s3_persistence.py       # S3 write operations
│   ├── resources/
│   │   ├── __init__.py
│   │   ├── mlflow_resource.py      # MLflow client resource
│   │   ├── duckdb_resource.py      # DuckDB connection resource
│   │   ├── s3_resource.py          # S3 client resource
│   │   └── api_clients.py          # Enterprise API clients
│   ├── sensors/
│   │   ├── __init__.py
│   │   └── mlflow_sensor.py        # Detect new MLflow runs
│   ├── schedules/
│   │   ├── __init__.py
│   │   └── pipeline_schedules.py   # Delta & historical schedules
│   ├── ops/
│   │   ├── __init__.py
│   │   ├── data_quality.py         # Data validation ops
│   │   └── notifications.py        # Alert/notification ops
│   ├── jobs/
│   │   ├── __init__.py
│   │   ├── delta_job.py           # 5-min incremental job
│   │   └── historical_job.py      # Daily full refresh job
│   └── repository.py               # Dagster repository definition
├── shared/
│   ├── __init__.py
│   ├── contracts.py               # Shared data contracts/models
│   └── constants.py               # Shared constants
├── .env.example
├── requirements.txt
└── README.md