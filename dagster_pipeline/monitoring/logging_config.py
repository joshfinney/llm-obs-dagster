from datetime import datetime
import logging
import logging.config
import sys
from typing import Any


def setup_logging(log_level: str = "INFO", enable_rich_logging: bool = True) -> None:
    """Configure comprehensive logging for the pipeline.

    Features:
    - Structured JSON logging for production
    - Rich console output for development
    - Separate loggers for different components
    - Performance metrics logging
    """
    # Define logging configuration
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S"
            },
            "json": {
                "format": '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s", "module": "%(module)s", "function": "%(funcName)s", "line": %(lineno)d}',
                "datefmt": "%Y-%m-%d %H:%M:%S"
            },
            "detailed": {
                "format": "%(asctime)s [%(levelname)8s] %(name)30s | %(funcName)20s:%(lineno)4d | %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": log_level,
                "formatter": "detailed" if enable_rich_logging else "standard",
                "stream": sys.stdout
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": log_level,
                "formatter": "json",
                "filename": f"logs/pipeline_{datetime.now().strftime('%Y%m%d')}.log",
                "maxBytes": 10485760,  # 10MB
                "backupCount": 5
            },
            "error_file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "ERROR",
                "formatter": "json",
                "filename": f"logs/errors_{datetime.now().strftime('%Y%m%d')}.log",
                "maxBytes": 10485760,
                "backupCount": 10
            }
        },
        "loggers": {
            # Root logger
            "": {
                "handlers": ["console", "file"],
                "level": log_level,
                "propagate": False
            },
            # Pipeline-specific loggers
            "dagster_pipeline.assets": {
                "handlers": ["console", "file"],
                "level": log_level,
                "propagate": False
            },
            "dagster_pipeline.resources": {
                "handlers": ["console", "file"],
                "level": log_level,
                "propagate": False
            },
            "dagster_pipeline.jobs": {
                "handlers": ["console", "file"],
                "level": log_level,
                "propagate": False
            },
            # External library loggers
            "requests": {
                "handlers": ["file"],
                "level": "WARNING",
                "propagate": False
            },
            "urllib3": {
                "handlers": ["file"],
                "level": "WARNING",
                "propagate": False
            },
            "mlflow": {
                "handlers": ["file"],
                "level": "INFO",
                "propagate": False
            },
            "dagster": {
                "handlers": ["console", "file"],
                "level": "INFO",
                "propagate": False
            },
            # Error logging
            "errors": {
                "handlers": ["error_file", "console"],
                "level": "ERROR",
                "propagate": False
            }
        }
    }

    # Create logs directory if it doesn't exist
    import os
    os.makedirs("logs", exist_ok=True)

    # Apply configuration
    logging.config.dictConfig(logging_config)

    # Log startup
    logger = logging.getLogger(__name__)
    logger.info("Pipeline logging initialized")
    logger.info(f"Log level: {log_level}")
    logger.info(f"Rich logging: {enable_rich_logging}")

class PipelineLogger:
    """Enhanced logger with pipeline-specific methods."""

    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.performance_logger = logging.getLogger(f"{name}.performance")

    def log_asset_start(self, asset_name: str, context: dict[str, Any] | None = None):
        """Log asset execution start."""
        self.logger.info(f"🚀 Starting asset: {asset_name}", extra={
            "asset_name": asset_name,
            "event_type": "asset_start",
            "context": context or {}
        })

    def log_asset_complete(self, asset_name: str, metrics: dict[str, Any] | None = None):
        """Log asset execution completion."""
        self.logger.info(f"✅ Completed asset: {asset_name}", extra={
            "asset_name": asset_name,
            "event_type": "asset_complete",
            "metrics": metrics or {}
        })

    def log_performance(self, operation: str, duration_ms: float, additional_metrics: dict[str, Any] | None = None):
        """Log performance metrics."""
        self.performance_logger.info(f"⏱️ {operation}: {duration_ms:.2f}ms", extra={
            "operation": operation,
            "duration_ms": duration_ms,
            "event_type": "performance",
            **(additional_metrics or {})
        })

    def log_data_quality(self, check_name: str, passed: bool, details: dict[str, Any] | None = None):
        """Log data quality checks."""
        status = "✅ PASSED" if passed else "❌ FAILED"
        level = logging.INFO if passed else logging.WARNING

        self.logger.log(level, f"{status} Data Quality: {check_name}", extra={
            "check_name": check_name,
            "passed": passed,
            "event_type": "data_quality",
            "details": details or {}
        })

    def log_api_call(self, api_name: str, endpoint: str, status_code: int, duration_ms: float):
        """Log API call metrics."""
        status = "✅" if 200 <= status_code < 300 else "❌"
        self.logger.info(f"{status} API Call: {api_name} {endpoint} ({status_code}) - {duration_ms:.2f}ms", extra={
            "api_name": api_name,
            "endpoint": endpoint,
            "status_code": status_code,
            "duration_ms": duration_ms,
            "event_type": "api_call"
        })

    def log_error(self, error: Exception, context: dict[str, Any] | None = None):
        """Log errors with context."""
        error_logger = logging.getLogger("errors")
        error_logger.error(f"💥 Error: {error!s}", extra={
            "error_type": type(error).__name__,
            "error_message": str(error),
            "event_type": "error",
            "context": context or {}
        }, exc_info=True)

def get_pipeline_logger(name: str) -> PipelineLogger:
    """Get a configured pipeline logger."""
    return PipelineLogger(name)
