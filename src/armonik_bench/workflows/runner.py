import json
import logging

from pathlib import Path

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException
from airflow.io.path import ObjectStoragePath
from airflow.models.param import Param
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.google.cloud.operators.compute import ComputeEngineDeleteInstanceOperator, ComputeEngineInsertInstanceOperator, ComputeEngineStartInstanceOperator, ComputeEngineStopInstanceOperator
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator


base = ObjectStoragePath(os.environ["AIRFLOW_OBJECT_STORAGE_PATH"])


@dag(
    dag_id="armonik-benchmark-runner",
    description="Workflow for running a given workload from an existing client on a given infrastructure.",
    schedule=None,
    params={
        "armonik_conn_id": Param(
            default="armonik_default",
            description="Reference to an existing or to be created gRPC Connection.",
            type="string",
        ),
        "github_conn_id": Param(
            default="github_default",
            description="Reference to a pre-defined GitHub Connection.",
            type="string",
        ),
        "bucket_prefix": Param(
            default="airflow-bench",
            description="Prefix of the S3/GCS bucket that will store the Terraform state file.",
            type="string",
        ),
        "gcp_conn_id": Param(
            default="google_cloud_default",
            description="Airflow connection to Google Cloud Platform.",
            type="string",
        ),
    },
)
def runner_dag():
    """Workflow assessing ArmoniK's performance against fault tolerance scenarios for the workload implemented by the ArmoniK HTC Mock client on a fixed infrastructure."""

    @task(task_id="get-metadata", multiple_outputs=True)
    def get_metadata(params: dict[str, str]) -> None:
        with (base / "metadata.json").open() as file:
            return json.loads(file.read())

    @task(task_id="list-experiments", multiple_outputs=False)
    def list_experiments(params: dict[str, str]) -> None:
        experiments = []

        for file in (base / "experiments").iterdir():
            if file.is_file():
                with file.open() as data:
                    try:
                        experiments.append(json.load(data.read()))
                    except json.JSONDecodeError:
                        raise AirflowFailException(f"Failed to load experiment described in {file}. Invalid JSON format.")

        return experiments

    list_experiments = list_experiments()

    @task_group()
    def deploy_client():
        @task(task_id="client-deployment-fork")
        def client_deployment_fork(ti: TaskInstance) -> None:
            match ti.xcom_pull(task_ids=get_metadata.task_id, key="environment"):
                case "localhost":
                    return deploy_client_localhost.task_id
                case "gcp":
                    return deploy_client_gcp.task_id

        client_deployment_fork = client_deployment_fork()

        deploy_client_localhost = EmptyOperator(task_id="deploy-client-localhost")

        deploy_client_gcp = ComputeEngineInsertInstanceOperator(
            task_id="demploy-client-gcp",
            resource_id="{{ ti.xcom_pull(task_ids='get-metadata', key='client')['instance_name'] }}",
            zone="{{ ti.xcom_pull(task_ids='get-metadata', key='client')['location'] }}",
            gcp_conn_id="{{ params.gcp_conn_id }}",
            body={
                "machine_type": "zones/{{ ti.xcom_pull(task_ids='get-metadata', key='client')['location'] }}/machineTypes/{{ ti.xcom_pull(task_ids='get-metadata', key='client')['machine_type'] }}",
                "disks": [
                    {
                        "autoDelete": True,
                        "boot": True,
                        "device_name": "{{ ti.xcom_pull(task_ids='get-metadata', key='client')['instance_name'] }}",
                        "initialize_params": {
                            "disk_size_gb": "10",
                            "disk_type": "zones/{{ ti.xcom_pull(task_ids='get-metadata', key='client')['location'] }}/diskTypes/pd-balanced",
                            "source_image": "projects/cos-cloud/global/images/cos-stable-109-17800-147-22",
                        },
                    }
                ],
                "network_interfaces": [
                    {
                        "access_configs": [{"name": "External NAT", "network_tier": "PREMIUM"}],
                        "stack_type": "IPV4_ONLY",
                        "subnetwork": "regions/{{ ti.xcom_pull(task_ids='get-metadata', key='client')['region'] }}/subnetworks/default",
                    }
                ]
            }
        )

        start_client_gcp = ComputeEngineStartInstanceOperator(
            task_id="start-client-gcp",
            zone="{{ ti.xcom_pull(task_ids='get-metadata', key='client')['zone'] }}",
            resource_id="{{ ti.xcom_pull(task_ids='get-metadata', key='client')['instance_name'] }}",
            gcp_conn_id="{{ params.gcp_conn_id }}",
        )

        client_deployment_fork >> [deploy_client_localhost, deploy_client_gcp]

        deploy_client_gcp >> start_client_gcp

    deploy_client = deploy_client()

    run_experiment = TriggerDagRunOperator.partial(
        task_id="run-experiment",
        trigger_dag_id="armonik-benchmark-run-experiment",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
    ).expand(
        conf=[
            {
                "release": experiment["release"],
                "environment": experiment["environment"],
                "parameters_file": experiment["parameters_file"],
                "workload": experiment["workload"],
                "workload_config": experiment["workload_config"],
                "client_instance_name": experiment["client_instance_name"],
                "client_instance_location": experiment["client_instance_location"],
            }
            for experiment in list_experiments
        ],
    )

    dump_db = EmptyOperator(task_id="dump-db")

    dump_logs = EmptyOperator(task_id="dump-logs")

    @task_group()
    def destroy_client():
        @task(task_id="client-destruction-fork")
        def client_destruction_fork(ti: TaskInstance) -> None:
            match ti.xcom_pull(task_ids=get_metadata.task_id, key="environment"):
                case "localhost":
                    return destroy_client_localhost.task_id
                case "gcp":
                    return destroy_client_gcp.task_id

        client_destruction_fork = client_destruction_fork()
        
        destroy_client_localhost = EmptyOperator(task_id="destroy-client-localhost")

        destroy_client_gcp = ComputeEngineDeleteInstanceOperator(
            task_id="destroy-client-gcp",
            zone="{{ ti.xcom_pull(task_ids='get-metadata', key='client')['zone'] }}",
            resource_id="{{ ti.xcom_pull(task_ids='get-metadata', key='client')['instance_name'] }}",
            gcp_conn_id="{{ params.gcp_conn_id }}",
        )

        client_destruction_fork >> [destroy_client_localhost, destroy_client_gcp]

    destroy_client = destroy_client()

    destroy_infra = TriggerDagRunOperator(
        task_id="destroy-infra",
        trigger_dag_id="armonik-infra-destroy",
        conf={
            "release": "{{ ti.xcom_pull(task_ids='get-metadata', key='release') }}",
            "environment": "{{ ti.xcom_pull(task_ids='get-metadata', key='environment') }}",
            "parameters_file": "{{ ti.xcom_pull(task_ids='list-experiments')[-1]['parameters_file'] }}",
        },
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=["success"],
    )

    (
        list_experiments
        >> deploy_client
        >> run_experiment
        >> [
            dump_db,
            dump_logs,
            destroy_client
        ]
        >> destroy_infra
    )


runner_dag()
