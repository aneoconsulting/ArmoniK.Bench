import json
import logging
import os

from datetime import datetime, timezone

import numpy as np

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException, AirflowException
from airflow.models.connection import Connection
from airflow.models.param import Param
from airflow.models.taskinstance import TaskInstance
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.grpc.hooks.grpc import GrpcHook
from airflow.sensors.base import PokeReturnValue
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
from kubernetes.client import models as k8s
from armonik_bench.common import (
    armonik_services_healthy,
    kubernetes_n_nodes_ready,
    update_workload_config,
)

from notifiers.notifier import ArmoniKBenchEmailNotifier
from operators.armonik import ArmoniKDeployClusterOperator, ArmoniKDestroyClusterOperator
from operators.extra_templated import ExtraTemplatedKubernetesJobOperator


from pathlib import Path

base = Path("/home/airflow/gcs/data")


@dag(
    dag_id="run_experiment",
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
        "exp_name": Param(
            default="",
            description="Name of the experiment to be run.",
            type="string",
        ),
        "release": Param(
            default="latest", description="ArmoniK release to be deployed.", type="string"
        ),
        "environment": Param(
            default="localhost",
            description="The environment in which to deploy the ArmoniK cluster.",
            type="string",
            enum=["localhost", "aws", "gcp"],
        ),
        "infra_config": Param(
            default={}, description="Infrastructure configuration in JSON format", type="object"
        ),
        "infra_region": Param(
            default="us-central1", description="Region of infrastructure deployment", type="string"
        ),
        "workload": Param(
            default="",
            description="Docker image of the workload to run with the proper tag.",
            type="string",
        ),
        "workload_config": Param(
            default={},
            description="Workload configuring to be provided as container environment variables.",
            type="object",
        ),
        "destroy": Param(
            default=False,
            description="Weither to destroy the cluster at the end of the experiment",
            type="boolean",
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
def run_experiment():
    """Workflow assessing ArmoniK's performance against fault tolerance scenarios for the workload implemented by the ArmoniK HTC Mock client on a fixed infrastructure."""

    deploy_armonik = ArmoniKDeployClusterOperator(
        task_id="deploy_armonik",
        release="{{ params.release }}",
        environment="{{ params.environment }}",
        region="{{ params.infra_region }}",
        config="{{ params.infra_config }}",
        armonik_conn_id="{{ params.armonik_conn_id }}",
        kubernetes_conn_id="{{ params.kubernetes_conn_id }}",
        github_conn_id="{{ params.github_conn_id }}",
        bucket_prefix="{{ params.bucket_prefix }}",
    )

    @task_group
    def warm_up():
        @task.sensor(poke_interval=10, timeout=600, mode="poke")
        def services_ready(params: dict[str, str]) -> PokeReturnValue:
            try:
                with GrpcHook(grpc_conn_id=params["armonik_conn_id"]).get_conn() as channel:
                    if armonik_services_healthy(channel=channel):
                        return PokeReturnValue(is_done=True)
                    return PokeReturnValue(is_done=False)
            except RpcError:
                return PokeReturnValue(is_done=False)

        services_ready()

        @task.sensor(poke_interval=5, timeout=600, mode="poke")
        def nodes_ready(params: dict[str, any]):
            logger = logging.getLogger("airflow.task")
            if kubernetes_n_nodes_ready(
                KubernetesHook(conn_id=params["kubernetes_conn_id"]).core_v1_client,
                params["infra_config"]["gke"]["node_pools"][0]["node_count"],
                ".*worker.*",
            ):
                logger.info("All nodes of the cluster are available.")
                return PokeReturnValue(is_done=True)
            logger.info("Not all nodes of the cluster are available.")
            return PokeReturnValue(is_done=False)

        nodes_ready()

    warm_up = warm_up()

    @task
    def prepare_workload_execution(
        ti: TaskInstance | None = None, params: dict[str, str] | None = None
    ) -> None:
        conn = Connection.get_connection_from_secrets(conn_id=params["armonik_conn_id"])
        ti.xcom_push(
            key="workload_config",
            value=update_workload_config(params["workload_config"], conn.host, conn.port),
        )

    prepare_workload_execution = prepare_workload_execution()

    run_client = ExtraTemplatedKubernetesJobOperator(
        task_id="run_client",
        name="run-client",
        namespace="armonik",
        labels={"app": "armonik", "service": "run-client", "type": "others"},
        image="{{ params.workload }}",
        env_vars="{{ ti.xcom_pull(task_ids='prepare_workload_execution', key='workload_config') }}",
        node_selector={"service": "others"},
        tolerations=[k8s.V1Toleration(effect="NO_SCHEDULE", key="service", value="others")],
        backoff_limit=1,
        completion_mode="NonIndexed",
        completions=1,
        parallelism=1,
        on_finish_action="delete_pod",
        get_logs=True,
    )

    @task
    def commit(ti: TaskInstance | None = None, params: dict[str, str] | None = None) -> None:
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

    commit = commit()

    @task.short_circuit(trigger_rule="all_done")
    def skip_destroy(params: dict[str, str] | None = None) -> bool:
        if params["destroy"]:
            return True
        return False

    skip_destroy = skip_destroy()

    destroy_armonik = ArmoniKDestroyClusterOperator(
        task_id="destroy_armonik",
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
        deploy_armonik
        >> warm_up
        >> prepare_workload_execution
        >> run_client
        >> commit
        >> skip_destroy
        >> destroy_armonik
    )


run_experiment()
