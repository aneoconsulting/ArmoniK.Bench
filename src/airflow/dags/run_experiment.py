import json
import logging
import os
import shutil
import yaml

from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException, AirflowException
from airflow.models.connection import Connection
from airflow.models.param import Param
from airflow.models.taskinstance import TaskInstance
from airflow.operators.bash import BashOperator
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
from armonik_bench.common import (
    armonik_services_healthy,
    kubernetes_n_nodes_ready,
    update_workload_config,
    setup_terraform_environment,
)
from git import exc
from grpc import RpcError

from notifiers.notifier import ArmoniKBenchEmailNotifier
from operators.extra_templated import ExtraTemplatedKubernetesJobOperator
from operators.connection import CreateOrUpdateConnectionOperator, DeleteConnectionOperator


base = Path(os.environ["AIRFLOW_DATA"])
repo_path = Path(os.environ.get("AIRFLOW_DATA", os.getcwd())) / "ArmoniK"

get_modules_cmd = (
    "git -c advice.detachedHead=false clone --branch $MODULES_VERSION $MODULES_SOURCE $MODULES_DIR"
)
terraform_init_cmd = 'terraform init -upgrade -reconfigure -backend-config="secret_suffix=$PREFIX" -var-file=$VERSIONS_FILE -var-file=$PARAMETERS_FILE -var-file=$EXTRA_PARAMETERS_FILE'
terraform_apply_cmd = "terraform apply -var-file=$VERSIONS_FILE -var-file=$PARAMETERS_FILE -var-file=$EXTRA_PARAMETERS_FILE -auto-approve"
terraform_output_cmd = "terraform output -json > $OUTPUT_DIR"
terraform_destroy_cmd = "terraform destroy -var-file=$VERSIONS_FILE -var-file=$PARAMETERS_FILE -var-file=$EXTRA_PARAMETERS_FILE -auto-approve"


def init_fct(params: dict[str, str]) -> dict[str, str]:
    try:
        return setup_terraform_environment(
            repo_path=repo_path,
            repo_url=params["repo_url"],
            ref=params["release"],
            environment=params["environment"],
            infra_config=params["infra_config"],
            prefix=params["bucket_prefix"],
            region=params["infra_region"],
        )
    except exc.GitCommandError as error:
        raise AirflowFailException(
            f"An error occured when cloning or checking out the repositor: {error}"
        )
    except TypeError as error:
        raise AirflowFailException(f"Invalid infrastructure configuration format: {error}")


def teardown_fct() -> None:
    if repo_path.exists() and repo_path.is_dir():
        shutil.rmtree(repo_path)


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
        "repo_url": Param(
            default="https://github.com/aneoconsulting/ArmoniK",
            description="URL of a public repository hosting ArmoniK.",
            type="string",
        ),
    },
)
def run_experiment():
    """Workflow assessing ArmoniK's performance against fault tolerance scenarios for the workload implemented by the ArmoniK HTC Mock client on a fixed infrastructure."""

    @task_group
    def deploy_armonik_cluster() -> None:
        bash_options = {
            "cwd": str(repo_path / "infrastructure/quick-deploy/{{ params.environment }}"),
            "append_env": True,
            "env": "{{ ti.xcom_pull(task_ids='deploy_armonik_cluster.init', key='env_vars') }}",
        }

        @task
        def init(params: dict[str, str], ti: TaskInstance) -> None:
            env_vars = init_fct(params)
            ti.xcom_push(key="env_vars", value=env_vars)

        init = init()

        get_modules = BashOperator(
            task_id="get_modules", bash_command=get_modules_cmd, **bash_options
        )

        terraform_init = BashOperator(
            task_id="terraform_init", bash_command=terraform_init_cmd, **bash_options
        )

        terraform_apply = BashOperator(
            task_id="terraform_apply", bash_command=terraform_apply_cmd, **bash_options
        )

        terraform_output = BashOperator(
            task_id="terraform_output", bash_command=terraform_output_cmd, **bash_options
        )

        @task_group
        def create_kubernetes_connection() -> None:
            @task(trigger_rule="one_success")
            def read_kubeconfig(ti: TaskInstance, params: dict[str, str]) -> None:
                try:
                    if params["environment"] == "localhost":
                        kubeconfig_path = Path(ti.xcom_pull(task_ids="deploy_armonik_cluster.init", key="env_vars")["KUBE_CONFIG_PATH"])
                    else:
                        kubeconfig_path = repo_path / f"infrastructure/quick-deploy/{params['environment']}/generated/kubeconfig"
                    with kubeconfig_path.open() as file:
                        ti.xcom_push(key="kubeconfig", value=json.dumps(json.dumps(yaml.safe_load(file.read()))))
                except FileNotFoundError as error:
                    raise AirflowFailException(f"Can't read deployment outputs: {error}")

            read_kubeconfig = read_kubeconfig()

            create_connection = CreateOrUpdateConnectionOperator(
                task_id="create_kubernetes_connection",
                conn_id="{{ params.kubernetes_conn_id }}",
                conn_type="kubernetes",
                description="Kubernetes connection for a remote ArmoniK cluster.",
                extra={
                    "in_cluster": False,
                    "kube_config": "{{ ti.xcom_pull(task_ids='deploy_armonik_cluster.create_kubernetes_connection.read_kubeconfig', key='kubeconfig') }}",
                },
            )

            read_kubeconfig >> create_connection

        create_kubernetes_connection = create_kubernetes_connection()

        @task_group
        def create_armonik_connection() -> None:
            @task(trigger_rule="one_success")
            def read_output(ti: TaskInstance) -> None:
                output_file = Path(ti.xcom_pull(task_ids="deploy_armonik_cluster.init", key="env_vars")["OUTPUT_DIR"])
                with output_file.open() as file:
                    data = json.loads(file.read())
                    ti.xcom_push(key="host", value=data['armonik']['value']['control_plane_url'].removeprefix('http://').split(':')[0])
                    ti.xcom_push(key="port", value=data['armonik']['value']['control_plane_url'].removeprefix('http://').split(':')[1])

            read_output = read_output()

            create_connection = CreateOrUpdateConnectionOperator(
                task_id="create_armonik_connection",
                conn_id="{{ params.armonik_conn_id }}",
                conn_type="grpc",
                description="Connection to ArmoniK control plane.",
                host="{{ ti.xcom_pull(task_ids='deploy_armonik_cluster.create_armonik_connection.read_output', key='host') }}",
                port="{{ ti.xcom_pull(task_ids='deploy_armonik_cluster.create_armonik_connection.read_output', key='port') }}",
                extra=json.dumps({"auth_type": "NO_AUTH"}),
            )

            read_output >> create_connection
        
        create_armonik_connection = create_armonik_connection()

        @task
        def teardown() -> None:
            teardown_fct()

        teardown = teardown()

        (
            init
            >> get_modules
            >> terraform_init
            >> terraform_apply
            >> terraform_output
            >> [create_armonik_connection, create_kubernetes_connection]
            >> teardown
        )

    @task_group
    def destroy_armonik_cluster() -> None:
        bash_options = {
            "cwd": str(repo_path / "infrastructure/quick-deploy/{{ params.environment }}"),
            "append_env": True,
            "env": "{{ ti.xcom_pull(task_ids='destroy_armonik_cluster.init', key='env_vars') }}",
        }

        @task
        def init(params: dict[str, str], ti: TaskInstance) -> None:
            env_vars = init_fct(params)
            ti.xcom_push(key="env_vars", value=env_vars)

        init = init()

        get_modules = BashOperator(
            task_id="get_modules", bash_command=get_modules_cmd, **bash_options
        )

        terraform_init = BashOperator(
            task_id="terraform_init", bash_command=terraform_init_cmd, **bash_options
        )

        terraform_destroy = BashOperator(
            task_id="terraform_destroy", bash_command=terraform_destroy_cmd, **bash_options
        )

        delete_connections = DeleteConnectionOperator(
            task_id="delete_connections",
            conn_ids=["{{ params.armonik_conn_id }}", "{{ params.kubernetes_conn_id }}"],
        )

        @task
        def teardown():
            teardown_fct()

        teardown = teardown()

        init >> get_modules >> terraform_init >> terraform_destroy >> delete_connections >> teardown

    deploy_armonik = deploy_armonik_cluster()

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
            if params["environment"] == "localhost":
                logger.info("Skip check because working on localhost environment.")
                return PokeReturnValue(is_done=True)
            n_nodes = params["infra_config"]["gke"]["node_pools"][0]["node_count"]
            if kubernetes_n_nodes_ready(
                KubernetesHook(conn_id=params["kubernetes_conn_id"]).core_v1_client,
                n_nodes,
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
            value=update_workload_config(params["workload_config"], conn.host, conn.port)[0],
        )
        ti.xcom_push(
            key="uuid",
            value=update_workload_config(params["workload_config"], conn.host, conn.port)[1],
        )

    prepare_workload_execution = prepare_workload_execution()

    run_client = ExtraTemplatedKubernetesJobOperator(
        task_id="run_client",
        name="run-client",
        namespace="armonik",
        labels={"app": "armonik", "service": "run-client", "type": "others"},
        image="{{ params.workload }}",
        env_vars="{{ ti.xcom_pull(task_ids='prepare_workload_execution', key='workload_config') }}",
        backoff_limit=1,
        completion_mode="NonIndexed",
        completions=1,
        parallelism=1,
        on_finish_action="delete_pod",
        get_logs=True,
        wait_until_job_complete=True,
    )

    @task
    def commit(ti: TaskInstance | None = None, params: dict[str, str] | None = None) -> None:
        try:
            logger = logging.getLogger("airflow.task")
            hook = GrpcHook(grpc_conn_id=params["armonik_conn_id"])
            with hook.get_conn() as channel:
                stats = ArmoniKStatistics(
                    channel=channel,
                    task_filter=TaskFieldFilter.task_options_key("UUID")
                    == ti.xcom_pull(task_ids="prepare_workload_execution", key="uuid"),
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
                    task_filter=TaskFieldFilter.task_options_key("UUID")
                    == ti.xcom_pull(task_ids="prepare_workload_execution", key="uuid"),
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

    destroy_armonik = destroy_armonik_cluster()

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
