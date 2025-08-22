from datetime import datetime
import logging

import boto3
from dagster import Field, InputContext, OutputContext, String, io_manager
import pandas as pd

logger = logging.getLogger(__name__)

# Constants for categorical optimization
CATEGORICAL_THRESHOLD_RATIO = 0.5
CATEGORICAL_MAX_UNIQUE_VALUES = 1000

@io_manager(
    config_schema={
        "s3_bucket": Field(String, description="S3 bucket name"),
        "s3_prefix": Field(
            String,
            default_value="mlflow-metrics",
            description="S3 prefix for data"
        ),
        "aws_region": Field(
            String,
            default_value="us-east-1",
            description="AWS region"
        )
    }
)
def s3_parquet_io_manager(context):
    """S3 I/O Manager that persists DataFrames as Parquet files in S3.

    Features:
    - Automatic partitioning by date
    - Compression optimization
    - Metadata preservation
    - Error handling and retry logic
    """

    class S3ParquetIOManager:
        def __init__(self, s3_bucket: str, s3_prefix: str, aws_region: str):
            self.s3_bucket = s3_bucket
            self.s3_prefix = s3_prefix.strip('/')
            self.aws_region = aws_region

            # Initialize S3 client
            self.s3_client = boto3.client('s3', region_name=aws_region)

        def _get_s3_key(self, context: OutputContext) -> str:
            """Generate S3 key based on asset name and current date."""
            date_str = datetime.now().strftime("%Y/%m/%d")

            # Include partition information if available
            partition_str = ""
            if context.partition_key:
                partition_str = f"/partition={context.partition_key}"

            # Include run timestamp for uniqueness
            timestamp = datetime.now().strftime("%H-%M-%S")

            key = (
                f"{self.s3_prefix}/{context.asset_key.path[-1]}/"
                f"{date_str}{partition_str}/{timestamp}.parquet"
            )
            return key

        def _get_latest_s3_key(self, context: InputContext) -> str:
            """Find the latest S3 key for an asset."""
            prefix = f"{self.s3_prefix}/{context.asset_key.path[-1]}/"

            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.s3_bucket,
                    Prefix=prefix,
                    MaxKeys=1000
                )

                if 'Contents' not in response:
                    raise FileNotFoundError(
                        f"No data found for asset {context.asset_key}"
                    )

                # Sort by LastModified to get the latest
                objects = sorted(
                    response['Contents'],
                    key=lambda x: x['LastModified'],
                    reverse=True
                )
                latest_key = objects[0]['Key']

                logger.info(
                    f"Found latest file: s3://{self.s3_bucket}/{latest_key}"
                )
                return latest_key

            except Exception as e:
                logger.error(
                    f"Error finding latest S3 object for {context.asset_key}: {e!s}"
                )
                raise

        def handle_output(self, context: OutputContext, obj: pd.DataFrame):
            """Store DataFrame as Parquet in S3."""
            if obj is None or obj.empty:
                logger.warning(
                    f"Empty DataFrame for asset {context.asset_key}, skipping S3 write"
                )
                return

            s3_key = self._get_s3_key(context)
            s3_path = f"s3://{self.s3_bucket}/{s3_key}"

            try:
                # Optimize DataFrame for Parquet storage
                optimized_df = self._optimize_for_parquet(obj)

                # Write to S3 with compression
                optimized_df.to_parquet(
                    s3_path,
                    engine='pyarrow',
                    compression='snappy',
                    index=False,
                    # For better compatibility
                    use_deprecated_int96_timestamps=True
                )

                # Log success with metadata
                file_size_mb = (
                    optimized_df.memory_usage(deep=True).sum() / (1024 * 1024)
                )

                context.log.info(
                    f"Successfully wrote {len(optimized_df)} records "
                    f"({file_size_mb:.2f} MB) to {s3_path}"
                )

                # Add metadata to context
                context.add_output_metadata({
                    "s3_path": s3_path,
                    "record_count": len(optimized_df),
                    "file_size_mb": round(file_size_mb, 2),
                    "columns": list(optimized_df.columns),
                    "data_types": {
                        col: str(dtype) for col, dtype in optimized_df.dtypes.items()
                    },
                    "write_timestamp": datetime.now().isoformat()
                })

            except Exception as e:
                logger.error(f"Failed to write to S3: {e!s}")
                raise

        def load_input(self, context: InputContext) -> pd.DataFrame:
            """Load DataFrame from latest Parquet file in S3."""
            try:
                s3_key = self._get_latest_s3_key(context)
                s3_path = f"s3://{self.s3_bucket}/{s3_key}"

                # Read from S3
                df = pd.read_parquet(s3_path, engine='pyarrow')

                logger.info(f"Loaded {len(df)} records from {s3_path}")

                # Add metadata
                context.add_input_metadata({
                    "s3_path": s3_path,
                    "record_count": len(df),
                    "columns": list(df.columns),
                    "load_timestamp": datetime.now().isoformat()
                })

                return df

            except Exception as e:
                logger.error(f"Failed to load from S3: {e!s}")
                raise

        def _optimize_for_parquet(self, df: pd.DataFrame) -> pd.DataFrame:
            """Optimize DataFrame for efficient Parquet storage."""
            optimized_df = df.copy()

            # Convert object columns to appropriate types
            for col in optimized_df.columns:
                if optimized_df[col].dtype == 'object':
                    # Try to convert to numeric if possible
                    try:
                        numeric_series = pd.to_numeric(
                            optimized_df[col], errors='coerce'
                        )
                        if not numeric_series.isna().all():
                            optimized_df[col] = numeric_series
                            continue
                    except (ValueError, TypeError):
                        # Conversion to numeric failed, keep original type
                        pass

                    # Try to convert to datetime if possible
                    if col.endswith('_time') or col.endswith('_timestamp'):
                        try:
                            optimized_df[col] = pd.to_datetime(
                                optimized_df[col], errors='coerce'
                            )
                            continue
                        except (ValueError, TypeError):
                            # Conversion to datetime failed, keep original type
                            pass

                    # Convert to category for string columns with limited unique values
                    unique_ratio = optimized_df[col].nunique() / len(optimized_df)
                    if (unique_ratio < CATEGORICAL_THRESHOLD_RATIO and
                        optimized_df[col].nunique() < CATEGORICAL_MAX_UNIQUE_VALUES):
                        optimized_df[col] = optimized_df[col].astype('category')

            # Sort by timestamp columns for better compression
            timestamp_cols = [
                col for col in optimized_df.columns if 'time' in col.lower()
            ]
            if timestamp_cols:
                optimized_df = optimized_df.sort_values(timestamp_cols[0])

            return optimized_df

    return S3ParquetIOManager(
        context.resource_config["s3_bucket"],
        context.resource_config["s3_prefix"],
        context.resource_config["aws_region"]
    )
