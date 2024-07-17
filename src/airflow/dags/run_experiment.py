"""
# Run Experiment DAG

## Objective
This Airflow DAG is designed to perform a single benchmark experiment for the computing platform
ArmoniK. The experiment involves deploying or updating the existing infrastructure, running a
workload on the deployed cluster, retrieving data from the workload run for future post-processing,
and optionally destroying the infrastructure based on specified parameters.

## Trigger
This DAG is not triggered automatically and does not catch up on missed runs. It is intended to be
triggered manually by another DAG or a user.

## Parameters
The DAG accepts the following parameters:
- `destroy`: A boolean parameter specifying whether infrastructure destruction is needed after the
  experiment. Default is False.

## Prerequisites
To run this DAG, the DAG must have the required credentials and associated permissions to deploy
ArmoniK on the specified environment. These credentials are the ones required by Terraform.

## Environment Variables
This DAG accepts the following optional environment variables:
- `DAG__RUN_EXPERIMENT__MAX_ACTIVE_TASKS`: Specifies the maximum number of active tasks allowed.
  Default is 10.
- `DAG__RUN_EXPERIMENT__DAGRUN_TIMEOUT`: Specifies the timeout for the DAG run. Default is 5 hours.

## Interactions
- **ArmoniK Cluster**: The DAG interacts with the deployed ArmoniK cluster to monitor, run
  workloads, and extract run data.
- **Google Cloud Storage**: Interaction occurs to store execution data and outputs.

## Failure Handling
In case of failure, the DAG will notify via email (see Airflow configuration).

## Outputs
The outputs of the DAG are stored in the 'data' folder of Google Cloud Storage bucket associated
with the Cloud Composer environment.
"""

import json
import logging
import os

from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException
from airflow.models import DagRun, Param, Variable
from kubernetes.client import models as k8s

from operators.connections import UpdateAirflowConnectionOperator, UpdateArmoniKClusterConnectionOperator
from operators.dump import ArmoniKDumpData
from operators.extra_templated import CustomKubernetesPodOperator as KubernetesPodOperator
from operators.run_client import RunArmoniKClientOperator
from operators.warm_up import ArmoniKServicesHealthCheckSensor, KubernetesNodesReadySensor

from utils import constants
from utils.filters import (
    get_control_plane_host_from_tf_outputs,
    get_control_plane_port_from_tf_outputs,
    get_kubernetes_cluster_arn_from_tf_output,
)


data_dir = Path(constants.COMPOSER_CORE_DATA_DIR)
context = os.environ["ARMONIK_BENCH__CONTEXT"]

terraform_pod_default_options = {
    "image": constants.TF_IMAGE,
    "namespace": constants.COMPOSER_K8S_USER_WORKLOAD_NAMESPACE,
    "volume_mounts": [
        k8s.V1VolumeMount(
            mount_path=constants.USER_PV_MOUNT_PATH, name=constants.USER_PV_MOUNT_NAME
        )
    ],
    "volumes": [
        k8s.V1Volume(
            name=constants.USER_PV_MOUNT_NAME,
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                claim_name=constants.USER_PVC_NAME
            ),
        )
    ],
    "reattach_on_restart": True,
    "on_finish_action": "delete_succeeded_pod",
    "config_file": constants.COMPOSER_K8S_KUBECONFIG,
    "kubernetes_conn_id": constants.COMPOSER_K8S_CONN_ID,
    "working_dir": constants.TF_WORKDIR,
    "env_vars": {
        "TF_DATA_DIR": constants.TF_DATA_DIR,
        "TF_PLUGIN_CACHE_DIR": constants.TF_PLUGIN_CACHE_DIR,
        "TF_BACKEND_BUCKET": constants.TF_BACKEND_BUCKET,
        "TF_VAR_prefix": constants.TF_VAR_PREFIX,
        "EXTRA_PARAMETERS_FILE": constants.TF_EXTRA_PARAMETERS_FILE,
        "VERSIONS_FILE": constants.TF_VERSIONS_FILE,
        "PARAMETERS_FILE": constants.TF_PARAMETERS_FILE,
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
    },
}

if context == "local":
    terraform_pod_default_options["service_account_name"] = "terraform-sa"


@dag(
    dag_id="run_experiment",
    start_date=constants.DAG_DEFAULT_START_DATE,
    schedule=None,
    catchup=False,
    # UI parameters
    description="Carry out a benchmark experiment on a given ArmoniK application and deployment",
    doc_md=__doc__,
    # Jinja templating parameters
    render_template_as_native_obj=True,
    user_defined_filters={
        "ak_host_from_tf": get_control_plane_host_from_tf_outputs,
        "ak_port_from_tf": get_control_plane_port_from_tf_outputs,
        "ak_cluster_arn_from_tf": get_kubernetes_cluster_arn_from_tf_output,
    },
    # Scaling parameters
    max_active_tasks=int(
        os.environ.get(
            "DAG__RUN_EXPERIMENT__MAX_ACTIVE_TASKS", constants.DAG_DEFAULT_MAX_ACTIVE_TASKS
        )
    ),
    max_active_runs=constants.DAG_DEFAULT_MAX_ACTIVE_RUNS,
    # Other paramters
    end_date=constants.DAG_DEFAULT_END_DATE,
    default_args={
        "owner": constants.DAG_DEFAULT_ARGS_OWNER,
        "retries": constants.DAG_DEFAULT_ARGS_RETRIES,
        "retry_delay": constants.DAG_DEFAULT_ARGS_RETRY_DELAY,
    },
    params={
        "experiment_id": Param(
            default="", description="ID of the experiment to be run by the workflow", type="string"
        ),
        "destroy": Param(
            default=False,
            description="Whether or not to destroy the ArmoniK cluster at the end of the experiment run",
            type="boolean",
        ),
    },
    dagrun_timeout=timedelta(minutes=int(os.environ["DAG__RUN_EXPERIMENT__DAGRUN_TIMEMOUT"]))
    if os.environ.get("DAG__RUN_EXPERIMENT__DAGRUN_TIMEMOUT", "")
    else constants.DAG_DEFAULT_DAGRUN_TIMEMOUT,
)
def run_experiment():
    @task
    def load_experiment(params: dict[str, str] | None = None) -> dict[str, str | dict[str, str]]:
        logger = logging.getLogger("airflow.task")
        experiment_id = params["experiment_id"]
        if not experiment_id:
            raise AirflowFailException("Experiment ID is empty.")
        logger.info(f"Loading experiment {experiment_id}.")

        with (data_dir / f"experiments/{experiment_id}").open() as experiment_file:
            experiment = json.loads(experiment_file.read())
        with (data_dir / f"environments/{experiment['environment']}").open() as environment_file:
            environment = json.loads(environment_file.read())
        with (data_dir / f"workloads/{experiment['workload']}").open() as workload_file:
            workload = json.loads(workload_file.read())
        Variable.set(key="environment_type", value=environment["type"])

        if environment["type"] == "localhost":
            infra_worker_nodes = 0
        elif environment["type"] == "gcp": 
            infra_worker_nodes = environment["config"]["gke"]["node_pools"][0]["node_count"]
        elif environment["type"] == "aws":
            infra_worker_nodes = 1
        else:
            raise ValueError(f"Environment not supported {environment['type']}.")
        return {
            "infra_config": json.dumps(environment["config"]),
            "infra_worker_nodes": infra_worker_nodes,
            "repo_url": environment["repo_url"],
            "repo_ref": environment["repo_ref"],
            "workload_image": workload["image"],
            "workload_config": workload["config"],
        }

    load_experiment = load_experiment()

    setup = KubernetesPodOperator(
        task_id="setup",
        name="setup",
        image="python:3.11.5",
        cmds=["bash", "-cx"],
        arguments=[
            r'python -c "import os;f = open(\"/tmp/workdir/setup.py\", \"w\"); '
            r'f.write(os.environ[\"_PYTHON_SCRIPT\"]); f.close()" && '
            "python /tmp/workdir/setup.py"
        ],
        namespace=terraform_pod_default_options["namespace"],
        volume_mounts=terraform_pod_default_options["volume_mounts"],
        volumes=terraform_pod_default_options["volumes"],
        reattach_on_restart=terraform_pod_default_options["reattach_on_restart"],
        on_finish_action=terraform_pod_default_options["on_finish_action"],
        config_file=terraform_pod_default_options["config_file"],
        kubernetes_conn_id=terraform_pod_default_options["kubernetes_conn_id"],
        env_vars={
            "_PYTHON_SCRIPT": (Path(__file__).parent / "scripts/setup.py").open().read(),
            "SETUP_SCRIPT__REPO_PATH": f"{constants.USER_PV_MOUNT_PATH}/ArmoniK",
            "SETUP_SCRIPT__REPO_URL": load_experiment["repo_url"],
            "SETUP_SCRIPT__REPO_REF": load_experiment["repo_ref"],
            "SETUP_SCRIPT__AK_CONFIG": load_experiment["infra_config"],
            "SETUP_SCRIPT__AK_ENVIRONMENT": "{{ var.value.environment_type }}",
            "SETUP_SCRIPT__CONTEXT": context,
        },
    )

    terraform_init = KubernetesPodOperator(
        task_id="terraform_init",
        name="terraform-init",
        cmds=["terraform"],
        arguments=[
            "init",
            "-upgrade",
            "-reconfigure",
        ],
        **terraform_pod_default_options,
    )

    terraform_apply = KubernetesPodOperator(
        task_id="terraform_apply",
        name="terraform-apply",
        cmds=["terraform"],
        arguments=[
            "apply",
            "-var-file=$(VERSIONS_FILE)",
            "-var-file=$(PARAMETERS_FILE)",
            "-var-file=$(EXTRA_PARAMETERS_FILE)",
            "-auto-approve",
        ],
        **terraform_pod_default_options,
    )

    terraform_output = KubernetesPodOperator(
        task_id="terraform_output",
        name="terraform-output",
        cmds=["sh", "-c"],
        arguments=[
            "mkdir -p /airflow/xcom && terraform output -state=$(STATE_FILE) -json > /airflow/xcom/return.json"
        ],
        env_vars={
            "STATE_FILE": constants.TF_BACKEND_STATE_FILE,
            "TF_DATA_DIR": constants.TF_DATA_DIR,
            "TF_PLUGIN_CACHE_DIR": constants.TF_PLUGIN_CACHE_DIR,
        },
        do_xcom_push=True,
        **{k: v for k, v in terraform_pod_default_options.items() if k != "env_vars"},
    )

    update_kube_connection = UpdateArmoniKClusterConnectionOperator(
        task_id="update_kube_connection",
        conn_id="armonik_kubernetes_default",
        description="Kubernetes connection for a remote ArmoniK cluster.",
        cluster_arn="{{ti.xcom_pull(task_ids='terraform_output', key='return_value') | ak_cluster_arn_from_tf}}",
    )

    update_armonik_connection = UpdateAirflowConnectionOperator(
        task_id="update_armonik_connection",
        conn_id="armonik_default",
        conn_type="grpc",
        description="Connection to ArmoniK control plane.",
        host="{{ti.xcom_pull(task_ids='terraform_output', key='return_value') | ak_host_from_tf}}",
        port="{{ti.xcom_pull(task_ids='terraform_output', key='return_value') | ak_port_from_tf}}",
        extra=json.dumps({"auth_type": "NO_AUTH"}),
    )

    @task_group
    def warm_up():
        KubernetesNodesReadySensor(
            task_id="worker_nodes_ready",
            n_nodes=load_experiment["infra_worker_nodes"],
            node_name_pattern=".*worker.*",
            kubernetes_conn_id="armonik_kubernetes_default",
            poke_interval=timedelta(seconds=10),
            timeout=timedelta(minutes=10),
        )

        ArmoniKServicesHealthCheckSensor(
            task_id="armonik_services_ready",
            armonik_conn_id="armonik_default",
            poke_interval=timedelta(seconds=10),
            timeout=timedelta(minutes=10),
        )

    warm_up = warm_up()

    run_client = RunArmoniKClientOperator(
        task_id="run_client",
        image=load_experiment["workload_image"],
        config=load_experiment["workload_config"],
    )

    dump_data = ArmoniKDumpData(
        task_id="dump_data",
        job_uuid=run_client.output["job_uuid"],
        data_dir=data_dir,
    )

    @task.short_circuit(trigger_rule="all_done", ignore_downstream_trigger_rules=False)
    def skip_destroy(params: dict[str, str] | None = None) -> bool:
        return params["destroy"]

    skip_destroy = skip_destroy()

    terraform_destroy = KubernetesPodOperator(
        task_id="terraform_destroy",
        name="terraform-destroy",
        cmds=["terraform"],
        arguments=[
            "destroy",
            "-var-file=$(VERSIONS_FILE)",
            "-var-file=$(PARAMETERS_FILE)",
            "-var-file=$(EXTRA_PARAMETERS_FILE)",
            "-auto-approve",
        ],
        **terraform_pod_default_options,
    )

    @task(trigger_rule="all_done")
    def commit(
        run_id: str, params: dict[str, str] | None = None, dag_run: DagRun | None = None
    ) -> None:
        with (data_dir / f"experiment_runs/{run_id}").open("w") as f:
            f.write(
                json.dumps(
                    {
                        "experiment_id": params["experiment_id"],
                        "start": str(dag_run.start_date),
                        "end": str(datetime.now()),
                        "status": str(dag_run.state),
                        "data_loc": str(data_dir / f"dumps/{run_id}.parquet"),
                    }
                )
            )

    commit = commit(run_id=dump_data.output["run_id"])

    (
        load_experiment
        >> setup
        >> terraform_init
        >> terraform_apply
        >> terraform_output
        >> [update_kube_connection, update_armonik_connection]
        >> warm_up
        >> run_client
        >> [skip_destroy, dump_data]
        >> terraform_destroy
        >> commit
    )


run_experiment()
