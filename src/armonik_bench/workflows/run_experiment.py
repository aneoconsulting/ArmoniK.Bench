import json
import logging
import os

from datetime import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowException
from airflow.io.path import ObjectStoragePath
from airflow.models.param import Param
from airflow.models.taskinstance import TaskInstance
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.grpc.hooks.grpc import GrpcHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from armonik.client import ArmoniKHealthChecks
from armonik.common import ServiceHealthCheckStatus
from armonik.client import TaskFieldFilter
from armonik_analytics import ArmoniKStatistics
from armonik_analytics.metrics import (
    AvgThroughput,
    TotalElapsedTime,
    TimestampsTransition,
    TasksInStatusOverTime,
)
from grpc import RpcError


base = ObjectStoragePath(os.environ["AIRFLOW_OBJECT_STORAGE_PATH"])


@dag(
    dag_id="armonik-run-experiment",
    description="Workflow for running a given workload from an existing client on a given infrastructure.",
    schedule=None,
    params={
        "release": Param(
            default="latest", description="ArmoniK release to be deployed.", type="string"
        ),
        "environment": Param(
            default="localhost",
            description="The environment in which to deploy the ArmoniK cluster.",
            type="string",
            enum=["localhost", "aws", "gcp"],
        ),
        "parameters_file": Param(
            description="Name of parameter file for infrastructure configuration", type="string"
        ),
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
        "workload": Param(
            description="Docker image of the workload to run with the proper tag.",
            type="string",
        ),
        "workload_config": Param(
            default={},
            description="Workload configuring to be provided as container environment variables.",
            type="object",
        ),
        "client_instance_name": Param(
            default="airflow-bench-client",
            description="GCP client instance name.",
            type="str",
        ),
        "client_instance_location": Param(
            default="europ-west1-c",
            description="GCP client instance location.",
            type="str",
        ),
        "gcp_conn_id": Param(
            default="google_cloud_default",
            description="Airflow connection to Google Cloud Platform.",
            type="string",
        ),
    },
)
def experiment_dag():
    """Workflow assessing ArmoniK's performance against fault tolerance scenarios for the workload implemented by the ArmoniK HTC Mock client on a fixed infrastructure."""

    update_cluster = TriggerDagRunOperator(
        task_id="deploy-cluster",
        trigger_dag_id="armonik-infra-deploy",
        conf={
            "release": "{{ params.release }}",
            "environment": "{{ params.environment }}",
            "parameters_file": "{{ params.parameters_file }}",
            "armonik_conn_id": "{{ params.armonik_conn_id }}",
            "github_conn_id": "{{ params.github_conn_id }}",
            "bucket_prefix": "{{ params.bucket_prefix }}",
        },
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=["success"],
    )

    @task(task_id="check-cluster-health")
    def check_cluster_health(params: dict[str, str]) -> None:
        try:
            logger = logging.getLogger("airflow.task")
            hook = GrpcHook(grpc_conn_id=params["armonik_conn_id"])
            with hook.get_conn() as channel:
                client = ArmoniKHealthChecks(channel)

                for name, health in client.check_health().items():
                    if health["status"] == ServiceHealthCheckStatus.HEALTHY:
                        logger.info(f"Service {name} is healthy.")
                    else:
                        raise AirflowFailException(
                            f"Service {name} is {health["status"]}: {health["message"]}."
                        )
        except RpcError as rpc_error:
            raise AirflowFailException(f"Failed to run cluster health checks, error: {rpc_error}")
        except Exception as e:
            raise AirflowException(f"ArmoniK operator error: {e}")

    check_cluster_health = check_cluster_health()

    @task(task_id="environment-fork")
    def environment_fork(params: dict[str, str]) -> None:
        match params["environment"]:
            case "localhost":
                return run_client_localhost.task_id
            case "gcp":
                return run_client_gcp.task_id

    environment_fork = environment_fork()

    run_client_localhost = DockerOperator(
        task_id="run-client-localhost",
        image="{{ params.workload }}",
        auto_remove=True,
        xcom_all=True,
        environment="{{ params.workload_config }}",
    )

    run_client_gcp = SSHOperator(
        task_id="run-client-gcp",
        ssh_hook=ComputeEngineSSHHook(
            user="username",
            instance_name="{{ params.client_instance_name }}",
            zone="{{ params.client_instance_location }}",
            use_oslogin=False,
            use_iap_tunnel=False,
            cmd_timeout=1,
            gcp_conn_id="{{ params.gcp_conn_id }}",
        ),
        command="docker run --rm \\ {% for key, value in params.workload_config.items() %} -e {{ key }}={{ value }} \\ {% endfor %} {{ params.workload }}",
    )

    @task(task_id="parse-client-output", trigger_rule="one_success")
    def parse_client_output(ti: TaskInstance, params: dict[str, str]) -> None:
        logs = ti.xcom_pull(task_ids=f'run-client-{params["environment"]}')
        ti.xcom_push(key="session-id", value=json.loads(logs[3])["sessionId"])

    parse_client_output = parse_client_output()

    @task(task_id="save-run-results")
    def save_run_results(ti: TaskInstance, params: dict[str, str]) -> None:
        try:
            logger = logging.getLogger("airflow.task")
            hook = GrpcHook(grpc_conn_id=params["armonik_conn_id"])
            with hook.get_conn() as channel:
                stats = ArmoniKStatistics(
                    channel=channel,
                    task_filter=TaskFieldFilter.SESSION_ID
                    == ti.xcom_pull(task_ids=parse_client_output.task_id, key="session-id"),
                    metrics=[
                        AvgThroughput(),
                        TotalElapsedTime(),
                        TimestampsTransition("created", "submitted"),
                        TimestampsTransition("submitted", "received"),
                        TimestampsTransition("received", "acquired"),
                        TimestampsTransition("acquired", "fetched"),
                        TimestampsTransition("fetched", "started"),
                        TimestampsTransition("started", "processed"),
                        TimestampsTransition("processed", "ended"),
                        TasksInStatusOverTime("processed", "ended"),
                        TasksInStatusOverTime("ended"),
                    ],
                )
                logger.info("Computing statistics...")
                stats.compute()
                run_file = (
                    base
                    / "runs"
                    / f"{params["exp_name"]}_{datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")}"
                )
                with run_file.open("w") as file:
                    file.write(json.dumps({name: value for name, value in stats.values.items()}))
        except RpcError as rpc_error:
            raise AirflowFailException(f"Failed to run cluster health checks, error: {rpc_error}")
        except Exception as e:
            raise AirflowException(f"ArmoniK operator error: {e}")

    save_run_results = save_run_results()

    (
        update_cluster
        >> check_cluster_health
        >> environment_fork
        >> [run_client_gcp, run_client_localhost]
        >> parse_client_output
        >> save_run_results
    )


experiment_dag()
