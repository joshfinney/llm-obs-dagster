from datetime import datetime
from http import HTTPStatus
import logging
import time

from dagster import Field, Int, String, resource
import requests

from shared.contracts import MetricMetadata, ThresholdConfig

logger = logging.getLogger(__name__)

class RetryableHTTPClient:
    """HTTP client with retry logic and rate limiting."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        max_retries: int = 3,
        timeout: int = 30
    ):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'User-Agent': 'dagster-mlflow-obs/1.0'
        })

    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make HTTP request with retry logic."""
        url = f"{self.base_url}{endpoint}"

        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.request(
                    method, url, timeout=self.timeout, **kwargs
                )

                if response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
                    retry_after = int(
                        response.headers.get('Retry-After', 5)
                    )
                    logger.warning(
                        f"Rate limited, waiting {retry_after}s "
                        f"before retry {attempt + 1}"
                    )
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()
                return response

            except requests.exceptions.RequestException as e:
                if attempt == self.max_retries:
                    logger.error(
                        f"Request failed after {self.max_retries + 1} attempts: {e!s}"
                    )
                    raise

                wait_time = 2 ** attempt  # Exponential backoff
                logger.warning(
                    f"Request failed, retrying in {wait_time}s "
                    f"(attempt {attempt + 1})"
                )
                time.sleep(wait_time)

        raise requests.exceptions.RequestException("Max retries exceeded")

    def get(self, endpoint: str, **kwargs) -> requests.Response:
        return self._make_request('GET', endpoint, **kwargs)

    def post(self, endpoint: str, **kwargs) -> requests.Response:
        return self._make_request('POST', endpoint, **kwargs)

    def put(self, endpoint: str, **kwargs) -> requests.Response:
        return self._make_request('PUT', endpoint, **kwargs)

@resource(
    config_schema={
        "api_url": Field(String, description="Metrics API base URL"),
        "api_key": Field(String, description="API authentication key"),
        "timeout": Field(Int, default_value=30),
        "max_retries": Field(Int, default_value=3)
    }
)
def metrics_api_resource(context):
    """Enterprise Metrics API client for fetching metric metadata.

    API Contract:
    GET /metrics/batch - Batch fetch metadata for multiple metrics
    GET /metrics/{metric_name} - Get single metric metadata
    GET /metrics - List all available metrics
    """

    class MetricsAPIClient:
        def __init__(
            self,
            api_url: str,
            api_key: str,
            timeout: int,
            max_retries: int
        ):
            self.client = RetryableHTTPClient(
                api_url, api_key, max_retries, timeout
            )

        def batch_get_metadata(self, metric_names: list[str]) -> list[MetricMetadata]:
            """Batch fetch metadata for multiple metrics."""
            logger.info(f"Fetching metadata for {len(metric_names)} metrics")

            try:
                response = self.client.post(
                    '/metrics/batch',
                    json={'metric_names': metric_names}
                )

                data = response.json()
                metadata_list = []

                for item in data.get('metrics', []):
                    try:
                        metadata = MetricMetadata(**item)
                        metadata_list.append(metadata)
                    except Exception as e:
                        logger.warning(
                            f"Failed to parse metadata for metric: "
                            f"{item.get('metric_name', 'unknown')} - {e!s}"
                        )

                logger.info(
                    f"Successfully fetched metadata for {len(metadata_list)} metrics"
                )
                return metadata_list

            except Exception as e:
                logger.error(f"Failed to fetch metrics metadata: {e!s}")
                raise

        def get_metric_metadata(self, metric_name: str) -> MetricMetadata | None:
            """Get metadata for a single metric."""
            try:
                response = self.client.get(f'/metrics/{metric_name}')
                data = response.json()
                return MetricMetadata(**data)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == HTTPStatus.NOT_FOUND:
                    logger.warning(f"Metric metadata not found: {metric_name}")
                    return None
                raise

        def list_available_metrics(self) -> list[str]:
            """List all available metrics."""
            try:
                response = self.client.get('/metrics')
                data = response.json()
                return data.get('metric_names', [])
            except Exception as e:
                logger.error(f"Failed to list available metrics: {e!s}")
                raise

    return MetricsAPIClient(
        context.resource_config["api_url"],
        context.resource_config["api_key"],
        context.resource_config["timeout"],
        context.resource_config["max_retries"]
    )

@resource(
    config_schema={
        "api_url": Field(String, description="Thresholds API base URL"),
        "api_key": Field(String, description="API authentication key"),
        "timeout": Field(Int, default_value=30),
        "max_retries": Field(Int, default_value=3)
    }
)
def thresholds_api_resource(context):
    """Enterprise Thresholds API client for managing metric thresholds.

    API Contract:
    POST /thresholds/batch - Batch fetch thresholds for metrics/experiments
    GET /thresholds - Get platform defaults
    POST /thresholds - Create/update threshold
    GET /thresholds/{metric_name} - Get thresholds for specific metric
    """

    class ThresholdsAPIClient:
        def __init__(
            self,
            api_url: str,
            api_key: str,
            timeout: int,
            max_retries: int
        ):
            self.client = RetryableHTTPClient(
                api_url, api_key, max_retries, timeout
            )

        def get_thresholds(
            self,
            metrics: list[str],
            experiments: list[str] | None = None
        ) -> list[ThresholdConfig]:
            """Batch fetch thresholds for metrics and experiments
            Returns both platform defaults and experiment-specific overrides.
            """
            logger.info(
                f"Fetching thresholds for {len(metrics)} metrics "
                f"across {len(experiments or [])} experiments"
            )

            try:
                payload = {
                    'metrics': metrics,
                    'experiments': experiments or []
                }

                response = self.client.post('/thresholds/batch', json=payload)
                data = response.json()

                thresholds_list = []
                for item in data.get('thresholds', []):
                    try:
                        # Parse timestamp strings
                        if isinstance(item.get('updated_at'), str):
                            item['updated_at'] = datetime.fromisoformat(
                                item['updated_at']
                            )

                        threshold = ThresholdConfig(**item)
                        thresholds_list.append(threshold)
                    except Exception as e:
                        logger.warning(
                            f"Failed to parse threshold config: {item} - {e!s}"
                        )

                logger.info(
                    f"Successfully fetched {len(thresholds_list)} threshold configurations"
                )
                return thresholds_list

            except Exception as e:
                logger.error(f"Failed to fetch thresholds: {e!s}")
                raise

        def update_threshold(
            self,
            metric_name: str,
            critical_threshold: float,
            warning_threshold: float,
            experiment_id: str | None = None,
            updated_by: str = "dagster-pipeline"
        ) -> ThresholdConfig:
            """Create or update threshold configuration."""
            try:
                payload = {
                    'metric_name': metric_name,
                    'critical_threshold': critical_threshold,
                    'warning_threshold': warning_threshold,
                    'experiment_id': experiment_id,
                    'updated_by': updated_by,
                    'updated_at': datetime.now().isoformat()
                }

                response = self.client.post('/thresholds', json=payload)
                data = response.json()

                # Parse response
                if isinstance(data.get('updated_at'), str):
                    data['updated_at'] = datetime.fromisoformat(data['updated_at'])

                return ThresholdConfig(**data)

            except Exception as e:
                logger.error(
                    f"Failed to update threshold for {metric_name}: {e!s}"
                )
                raise

        def get_platform_defaults(self) -> list[ThresholdConfig]:
            """Get all platform-level default thresholds."""
            try:
                response = self.client.get('/thresholds?level=platform')
                data = response.json()

                thresholds = []
                for item in data.get('thresholds', []):
                    if isinstance(item.get('updated_at'), str):
                        item['updated_at'] = datetime.fromisoformat(
                            item['updated_at']
                        )
                    thresholds.append(ThresholdConfig(**item))

                return thresholds

            except Exception as e:
                logger.error(f"Failed to fetch platform defaults: {e!s}")
                raise

    return ThresholdsAPIClient(
        context.resource_config["api_url"],
        context.resource_config["api_key"],
        context.resource_config["timeout"],
        context.resource_config["max_retries"]
    )

@resource(
    config_schema={
        "tracking_uri": Field(String, description="MLflow tracking URI"),
        "registry_uri": Field(
            String,
            is_required=False,
            description="MLflow model registry URI"
        ),
        "timeout": Field(Int, default_value=30)
    }
)
def settings_resource(context):
    """Settings resource to provide configuration to assets."""
    from dagster_pipeline.config.settings import Settings

    # Create settings instance that can be used by assets
    settings = Settings()
    return settings
