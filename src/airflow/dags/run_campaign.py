import json
import os

from datetime import datetime, timezone
from hashlib import sha256

from airflow.decorators import dag, task
from airflow.models.param import Param

from operators.run_experiment import RunExperiment
from notifiers.notifier import ArmoniKBenchEmailNotifier

from pathlib import Path

base = Path("/home/qdelamea/airflow_bench_data")


@dag(
    dag_id="run_campaign",
    description="Workflow for running a given workload from an existing client on a given infrastructure.",
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    max_active_runs=1,
    render_template_as_native_obj=True,
    template_searchpath=os.environ["AIRFLOW_TEMPLATE_SEARCHPATH"],
    on_success_callback=ArmoniKBenchEmailNotifier(
        aws_conn_id="aws_default",
        target_arn=os.environ["AIRFLOW_AWS_SNS_ARN"],
        message="./notifications/email_dag_success.html",
        subject="Airflow DAG execution success",
        region_name=os.environ["AIRFLOW_AWS_SNS_REGION"],
    ),
    default_args={
        "on_failure_callback": ArmoniKBenchEmailNotifier(
            aws_conn_id="aws_default",
            target_arn=os.environ["AIRFLOW_AWS_SNS_ARN"],
            message="./notifications/email_task_failure.html",
            subject="Airflow Task execution failure",
            region_name=os.environ["AIRFLOW_AWS_SNS_REGION"],
        )
    },
    params={
        "armonik_conn_id": Param(
            default="armonik_default",
            description="Reference to an existing or to be created gRPC Connection.",
            type="string",
        ),
        "kubernetes_conn_id": Param(
            default="kubernetes_default",
            description="Reference to an existing or to be created Kubernetes Connection.",
            type="string",
        ),
        "gcp_conn_id": Param(
            default="google_cloud_default",
            description="Airflow connection to Google Cloud Platform.",
            type="string",
        ),
        "bucket_prefix": Param(
            default="airflow-bench",
            description="Prefix of the S3/GCS bucket that will store the Terraform state file.",
            type="string",
        ),
        "campaign": Param(
            default="",
            description="Reference to the campaign to run (its hash).",
            type="string",
        ),
    },
)
def run_campaign():
    """Workflow assessing ArmoniK's performance against fault tolerance scenarios for the workload implemented by the ArmoniK HTC Mock client on a fixed infrastructure."""

    @task(multiple_outputs=True)
    def load_campaign(params: dict[str, str] | None = None) -> None:
        if params["campaign"]:
            with (base / f"campaigns/{params['campaign']}").open() as file:
                campaign_data = json.loads(file.read())

            experiments = []

            for experiment in campaign_data["experiments"]:
                with (base / f"workloads/{experiment['workload']}").open() as file:
                    workload = json.loads(file.read())
                with (base / f"infrastructures/{experiment['infrastructure']}").open() as file:
                    infrastructure = json.loads(file.read())
                experiments.append(
                    {
                        "name": sha256(json.dumps(experiment).encode("utf-8")).hexdigest(),
                        "workload": workload,
                        "infrastructure": infrastructure,
                    }
                )

            campaign_data["experiments"] = experiments
            return campaign_data

    load_campaign = load_campaign()

    run_experiments = RunExperiment(
        task_id="run_experiments",
        campaign_data="{{ ti.xcom_pull(task_ids='load_campaign')}}",
        stop_on_failure=True,
        allowed_failures=0,
        poke_interval=10,
        trigger_rule="one_success",
    )

    (load_campaign >> run_experiments)


run_campaign()
