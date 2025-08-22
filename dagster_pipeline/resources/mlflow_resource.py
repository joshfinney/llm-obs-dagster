from dagster import resource, Field, String
import mlflow
from mlflow.tracking import MlflowClient

@resource(
    config_schema={
        "tracking_uri": Field(String, description="MLflow tracking URI"),
        "registry_uri": Field(String, is_required=False)
    }
)
def mlflow_resource(context):
    """
    MLflow client resource with connection pooling
    Methods to implement:
    - list_experiments()
    - search_runs(experiment_ids, filter_string, max_results)
    - get_metric_history(run_id, metric_key)
    - get_run(run_id)
    """
    mlflow.set_tracking_uri(context.resource_config["tracking_uri"])
    return MlflowClient(
        tracking_uri=context.resource_config["tracking_uri"],
        registry_uri=context.resource_config.get("registry_uri")
    )