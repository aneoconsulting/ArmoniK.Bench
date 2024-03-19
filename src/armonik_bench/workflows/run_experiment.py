import json
import logging
import os

from datetime import datetime

import numpy as np

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException, AirflowException
from airflow.io.path import ObjectStoragePath
from airflow.models.connection import Connection
from airflow.models.param import Param
from airflow.models.taskinstance import TaskInstance
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.grpc.hooks.grpc import GrpcHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sensors.base import PokeReturnValue
from armonik.client import ArmoniKHealthChecks
from armonik.common import ServiceHealthCheckStatus
from armonik.client import ArmoniKTasks, TaskFieldFilter
from armonik_analytics import ArmoniKStatistics
from armonik_analytics.metrics import (
    AvgThroughput,
    TotalElapsedTime,
    TimestampsTransition,
    TasksInStatusOverTime,
)
from armonik_analytics.utils import TaskTimestamps
from grpc import RpcError

from notifications.notifier import ArmoniKBenchEmailNotifier
from operators.armonik import ArmoniKDeployClusterOperator


base = ObjectStoragePath(os.environ["AIRFLOW_OBJECT_STORAGE_PATH"])


@dag(
    dag_id="armonik-run-experiment",
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
        "exp_name": Param(
            description="Name of the experiment to be run.",
            type="string",
        ),
        "release": Param(
            default="latest", description="ArmoniK release to be deployed.", type="string"
        ),
        "environment": Param(
            description="The environment in which to deploy the ArmoniK cluster.",
            type="string",
            enum=["localhost", "aws", "gcp"],
        ),
        "infra_config": Param(
            description="Infrastructure configuration in JSON format", type="object"
        ),
        "infra_region": Param(description="Region of infrastructure deployment", type="string"),
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
            type="string",
        ),
        "client_instance_zone": Param(
            description="GCP client instance zone.",
            type="string",
        ),
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
def experiment_dag():
    """Workflow assessing ArmoniK's performance against fault tolerance scenarios for the workload implemented by the ArmoniK HTC Mock client on a fixed infrastructure."""

    update_cluster = ArmoniKDeployClusterOperator(
        task_id="deploy-cluster",
        release="{{ params.release }}",
        environment="{{ params.environment }}",
        region="{{ params.infra_region }}",
        config="{{ params.infra_config }}",
        armonik_conn_id="{{ params.armonik_conn_id }}",
        kubernetes_conn_id="{{ params.kubernetes_conn_id }}",
        github_conn_id="{{ params.github_conn_id }}",
        bucket_prefix="{{ params.bucket_prefix }}",
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
                            f"Service {name} is {health['status']}: {health['message']}."
                        )
        except RpcError as rpc_error:
            raise AirflowFailException(f"Failed to run cluster health checks, error: {rpc_error}")
        except Exception as e:
            raise AirflowException(f"ArmoniK operator error: {e}")

    check_cluster_health = check_cluster_health()

    @task(task_id="retrive-cluster-connection")
    def retrieve_cluster_connection(ti: TaskInstance, params: dict[str, str]) -> None:
        workload_config = {key: value for key, value in params["workload_config"].items()}
        conn = Connection.get_connection_from_secrets(conn_id=params["armonik_conn_id"])
        workload_config["GrpcClient__Endpoint"] = (
            f"http://{conn.host}" if not conn.port else f"http://{conn.host}:{conn.port}"
        )
        ti.xcom_push(key="workload_config", value=workload_config)

    retrieve_cluster_connection = retrieve_cluster_connection()

    @task.branch(task_id="environment-fork")
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
        auto_remove="success",
        xcom_all=True,
        environment="{{ ti.xcom_pull(task_ids='retrive-cluster-connection', key='workload_config') }}",
    )

    @task_group
    def run_client_gcp():
        @task.sensor(task_id="wait-for-cluster-nodes", poke_interval=5, timeout=600, mode="poke")
        def wait_for_cluster_nodes(params: dict[str, any]):
            n_nodes = params["infra_config"]["gke"]["node_pools"][0]["node_count"]
            logger = logging.getLogger("airflow.task")
            logger.info(f"Waiting for the {n_nodes} nodes to be available...")
            hook = KubernetesHook(conn_id=params["kubernetes_conn_id"])
            node_names = [node.metadata.name for node in hook.core_v1_client.list_node().items]
            if n_nodes == len(node_names):
                logger.info("All the nodes are available.")
                return PokeReturnValue(is_done=True, xcom_value=node_names)
            logger.info(f"{n_nodes - len(node_names)} nodes are still not available.")
            return PokeReturnValue(is_done=False)

        wait_for_cluster_nodes = wait_for_cluster_nodes()
        
        run_client = SSHOperator(
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
            command="docker run --rm \\ {% for key, value in ti.xcom_pull(task_ids='retrive-cluster-connection', key='workload_config').items() %} -e {{ key }}={{ value }} \\ {% endfor %} {{ params.workload }}",
        )

        wait_for_cluster_nodes >> run_client

    @task(task_id="parse-client-output", trigger_rule="one_success")
    def parse_client_output(ti: TaskInstance, params: dict[str, str]) -> None:
        logs = ti.xcom_pull(task_ids=f'run-client-{params["environment"]}')
        for log in reversed(logs):
            try:
                log = json.loads(log)
                session_id = log["sessionId"]
                break
            except json.decoder.JSONDecodeError:
                continue
            except KeyError:
                continue
        ti.xcom_push(key="session-id", value=session_id)

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
                    == ti.xcom_pull(task_ids="parse-client-output", key="session-id"),
                    metrics=[
                        AvgThroughput(),
                        TotalElapsedTime(),
                        TimestampsTransition(TaskTimestamps.CREATED, TaskTimestamps.SUBMITTED),
                        TimestampsTransition(TaskTimestamps.SUBMITTED, TaskTimestamps.RECEIVED),
                        TimestampsTransition(TaskTimestamps.RECEIVED, TaskTimestamps.ACQUIRED),
                        TimestampsTransition(TaskTimestamps.ACQUIRED, TaskTimestamps.FETCHED),
                        TimestampsTransition(TaskTimestamps.FETCHED, TaskTimestamps.STARTED),
                        TimestampsTransition(TaskTimestamps.STARTED, TaskTimestamps.PROCESSED),
                        TimestampsTransition(TaskTimestamps.PROCESSED, TaskTimestamps.ENDED),
                        TasksInStatusOverTime(TaskTimestamps.ENDED),
                    ],
                )
                logger.info("Computing statistics...")
                stats.compute()
                tasks_by_status = ArmoniKTasks(channel).count_tasks_by_status(
                    TaskFieldFilter.SESSION_ID
                    == ti.xcom_pull(task_ids="parse-client-output", key="session-id")
                )
                run_file = (
                    base
                    / "runs"
                    / f"{params['exp_name']}_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')}"
                )
                with run_file.open("w") as file:
                    data = {}
                    for name, value in stats.values.items():
                        if isinstance(value, np.ndarray):
                            data[name] = {
                                "x": [(x - value[0, 0]).total_seconds() for x in value[0, :]],
                                "y": value[1, :].tolist(),
                            }
                        else:
                            data[name] = value
                    data["counts"] = {
                        status.name: count for status, count in tasks_by_status.items()
                    }
                    file.write(json.dumps(data))
        except RpcError as rpc_error:
            raise AirflowFailException(f"Failed to run cluster health checks, error: {rpc_error}")
        except Exception as e:
            raise AirflowException(f"ArmoniK operator error: {e}")

    save_run_results = save_run_results()

    (
        update_cluster
        >> check_cluster_health
        >> retrieve_cluster_connection
        >> environment_fork
        >> [run_client_gcp, run_client_localhost]
        >> parse_client_output
        >> save_run_results
    )


experiment_dag()
