import json
import os
from hashlib import sha256

from airflow.decorators import dag, task, task_group
from airflow.io.path import ObjectStoragePath
from airflow.models.param import Param
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineDeleteInstanceOperator,
    ComputeEngineInsertInstanceOperator,
    ComputeEngineStartInstanceOperator,
)

from notifications.notifier import ArmoniKBenchEmailNotifier
from operators.armonik import ArmoniKDestroyClusterOperator
from operators.run_experiment import RunExperiment


base = ObjectStoragePath(os.environ["AIRFLOW_OBJECT_STORAGE_PATH"])


@dag(
    dag_id="armonik-benchmark-runner",
    description="Workflow for running a given workload from an existing client on a given infrastructure.",
    schedule=None,
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
        "github_conn_id": Param(
            default="github_default",
            description="Reference to a pre-defined GitHub Connection.",
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
            description="Reference to the campaign to run (its hash).",
            type="string",
        ),
    },
)
def runner_dag():
    """Workflow assessing ArmoniK's performance against fault tolerance scenarios for the workload implemented by the ArmoniK HTC Mock client on a fixed infrastructure."""

    @task(task_id="load-campaign", multiple_outputs=True)
    def load_campaign(params: dict[str, str]) -> None:
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

    @task_group()
    def deploy_client():
        @task.branch(task_id="client-deployment-fork")
        def client_deployment_fork(ti: TaskInstance) -> None:
            match ti.xcom_pull(task_ids="load-campaign", key="environment"):
                case "localhost":
                    return deploy_client_localhost.task_id
                case "gcp":
                    return deploy_client_gcp.task_id

        client_deployment_fork = client_deployment_fork()

        deploy_client_localhost = EmptyOperator(task_id="deploy-client-localhost")

        deploy_client_gcp = ComputeEngineInsertInstanceOperator(
            task_id="demploy-client-gcp",
            zone="{{ ti.xcom_pull(task_ids='load-campaign', key='client')['instance_zone'] }}",
            gcp_conn_id="{{ params.gcp_conn_id }}",
            body={
                "name": "{{ ti.xcom_pull(task_ids='load-campaign', key='client')['instance_name'] }}",
                "machine_type": "zones/{{ ti.xcom_pull(task_ids='load-campaign', key='client')['instance_zone'] }}/machineTypes/{{ ti.xcom_pull(task_ids='load-campaign', key='client')['machine_type'] }}",
                "disks": [
                    {
                        "boot": True,
                        "device_name": "{{ ti.xcom_pull(task_ids='load-campaign', key='client')['instance_name'] }}",
                        "initialize_params": {
                            "disk_size_gb": "10",
                            "disk_type": "zones/{{ ti.xcom_pull(task_ids='load-campaign', key='client')['instance_zone'] }}/diskTypes/pd-balanced",
                            "source_image": "projects/cos-cloud/global/images/cos-stable-109-17800-147-22",
                        },
                    }
                ],
                "network_interfaces": [
                    {
                        "access_configs": [{"name": "External NAT", "network_tier": "PREMIUM"}],
                        "stack_type": "IPV4_ONLY",
                        "subnetwork": "regions/{{ ti.xcom_pull(task_ids='load-campaign', key='client')['instance_region'] }}/subnetworks/default",
                    }
                ],
            },
        )

        start_client_gcp = ComputeEngineStartInstanceOperator(
            task_id="start-client-gcp",
            zone="{{ ti.xcom_pull(task_ids='load-campaign', key='client')['instance_zone'] }}",
            resource_id="{{ ti.xcom_pull(task_ids='load-campaign', key='client')['instance_name'] }}",
            gcp_conn_id="{{ params.gcp_conn_id }}",
        )

        client_deployment_fork >> [deploy_client_localhost, deploy_client_gcp]

        deploy_client_gcp >> start_client_gcp

    deploy_client = deploy_client()

    run_experiments = RunExperiment(
        task_id="run-experiments",
        campaign_data="{{ ti.xcom_pull(task_ids='load-campaign')}}",
        stop_on_failure=True,
        allowed_failures=0,
        poke_interval=10,
        trigger_rule="one_success",
    )

    dump_db = EmptyOperator(task_id="dump-db")

    dump_logs = EmptyOperator(task_id="dump-logs")

    @task_group()
    def destroy_client():
        @task.branch(task_id="client-destruction-fork")
        def client_destruction_fork(ti: TaskInstance) -> None:
            match ti.xcom_pull(task_ids="load-campaign", key="environment"):
                case "localhost":
                    return destroy_client_localhost.task_id
                case "gcp":
                    return destroy_client_gcp.task_id

        client_destruction_fork = client_destruction_fork()

        destroy_client_localhost = EmptyOperator(task_id="destroy-client-localhost")

        destroy_client_gcp = ComputeEngineDeleteInstanceOperator(
            task_id="destroy-client-gcp",
            zone="{{ ti.xcom_pull(task_ids='load-campaign', key='client')['instance_zone'] }}",
            resource_id="{{ ti.xcom_pull(task_ids='load-campaign', key='client')['instance_name'] }}",
            gcp_conn_id="{{ params.gcp_conn_id }}",
        )

        client_destruction_fork >> [destroy_client_localhost, destroy_client_gcp]

    destroy_client = destroy_client()

    destroy_infra = ArmoniKDestroyClusterOperator(
        task_id="destroy-infra",
        release="{{ ti.xcom_pull(task_ids='load-campaign', key='release') }}",
        environment="{{ ti.xcom_pull(task_ids='load-campaign', key='environment') }}",
        region="{{ ti.xcom_pull(task_ids='load-campaign')['experiments'][-1]['infrastructure']['region'] }}",
        config="{{ ti.xcom_pull(task_ids='load-campaign')['experiments'][-1]['infrastructure']['config'] }}",
        armonik_conn_id="{{ params.armonik_conn_id }}",
        github_conn_id="{{ params.github_conn_id }}",
        bucket_prefix="{{ params.bucket_prefix }}",
        trigger_rule="one_success",
    )

    (
        load_campaign
        >> deploy_client
        >> run_experiments
        >> [dump_db, dump_logs, destroy_client]
        >> destroy_infra
    )


runner_dag()
