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

from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartJobOperator
from kubernetes.client import models as k8s

from operators.connections import UpdateAirflowConnectionOperator
from utils.kubeconfig import generate_gke_kube_config
from utils.filters import (
    get_control_plane_host_from_tf_outputs,
    get_control_plane_port_from_tf_outputs,
)


data_dir = Path("/home/airflow/gcs/data")


@dag(
    dag_id="run_experiment",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
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
    },
    # Scaling parameters
    max_active_tasks=int(os.environ.get("DAG__RUN_EXPERIMENT__MAX_ACTIVE_TASKS", 10)),
    max_active_runs=1,
    # Other paramters
    end_date=None,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
    params={
        "experiment_id": Param(
            default="", description="ID of the experiment to be run by the workflow", type="string"
        )
    },
    dagrun_timeout=timedelta(
        minutes=int(os.environ.get("DAG__RUN_EXPERIMENT__DAGRUN_TIMEMOUT", 300))
    ),
)
def run_experiment():
    @task(multiple_outputs=True)
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
        return {
            "infra_environment": environment["type"],
            "infra_region": environment["region"],
            "infra_config": json.dumps(environment["config"]),
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
        namespace="composer-user-workloads",
        volume_mounts=[k8s.V1VolumeMount(mount_path="/tmp/workdir", name="pvc-workdir-vol")],
        volumes=[
            k8s.V1Volume(
                name="pvc-workdir-vol",
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                    claim_name="pvc-workdir"
                ),
            )
        ],
        reattach_on_restart=True,
        on_finish_action="delete_succeeded_pod",
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id="kubernetes_default",
        env_vars={
            "_PYTHON_SCRIPT": (Path(__file__).parent / "scripts/setup.py").open().read(),
            "SETUP_SCRIPT__REPO_REF": load_experiment["repo_ref"],
            "SETUP_SCRIPT__AK_CONFIG": load_experiment["infra_config"],
            "SETUP_SCRIPT__REPO_PATH": "/tmp/workdir/ArmoniK",
            "SETUP_SCRIPT__REPO_URL": load_experiment["repo_url"],
            "SETUP_SCRIPT__AK_ENVIRONMENT": load_experiment["infra_environment"],
        },
    )

    terraform_init = KubernetesPodOperator(
        task_id="terraform_init",
        name="terraform-init",
        image="hashicorp/terraform:1.8",
        cmds=["terraform"],
        arguments=["init", "-upgrade", "-reconfigure", "-backend-config=bucket=$(PREFIX)-tfstate"],
        namespace="composer-user-workloads",
        volume_mounts=[k8s.V1VolumeMount(mount_path="/tmp/workdir", name="pvc-workdir-vol")],
        volumes=[
            k8s.V1Volume(
                name="pvc-workdir-vol",
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                    claim_name="pvc-workdir"
                ),
            )
        ],
        reattach_on_restart=True,
        on_finish_action="delete_succeeded_pod",
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id="kubernetes_default",
        full_pod_spec=k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        working_dir="/tmp/workdir/ArmoniK/infrastructure/quick-deploy/gcp",
                        name="terraform",
                    )
                ]
            )
        ),
        env_vars={
            "PREFIX": "airflow-bench",
            "TF_DATA_DIR": "/tmp/workdir/ArmoniK/infrastructure/quick-deploy/gcp/generated",
            "TF_PLUGIN_CACHE_DIR": "/tmp/workdir/ArmoniK/infrastructure/quick-deploy/gcp/generated/terraform-plugins",
            "TF_VAR_region": load_experiment["infra_region"],
            "TF_VAR_namespace": "armonik",
            "TF_VAR_prefix": "airflow-bench",
            "TF_VAR_project": "armonik-gcp-13469",
        },
    )

    terraform_apply = KubernetesPodOperator(
        task_id="terraform_apply",
        name="terraform-apply",
        image="hashicorp/terraform:1.8",
        cmds=["terraform"],
        arguments=[
            "apply",
            "-var-file=$(VERSIONS_FILE)",
            "-var-file=$(PARAMETERS_FILE)",
            "-var-file=$(EXTRA_PARAMETERS_FILE)",
            "-auto-approve",
        ],
        namespace="composer-user-workloads",
        volume_mounts=[k8s.V1VolumeMount(mount_path="/tmp/workdir", name="pvc-workdir-vol")],
        volumes=[
            k8s.V1Volume(
                name="pvc-workdir-vol",
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                    claim_name="pvc-workdir"
                ),
            )
        ],
        reattach_on_restart=True,
        on_finish_action="delete_succeeded_pod",
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id="kubernetes_default",
        full_pod_spec=k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        working_dir="/tmp/workdir/ArmoniK/infrastructure/quick-deploy/gcp",
                        name="terraform",
                    )
                ]
            )
        ),
        env_vars={
            "PREFIX": "airflow-bench",
            "TF_DATA_DIR": "/tmp/workdir/ArmoniK/infrastructure/quick-deploy/gcp/generated",
            "TF_PLUGIN_CACHE_DIR": "/tmp/workdir/ArmoniK/infrastructure/quick-deploy/gcp/generated/terraform-plugins",
            "TF_VAR_region": load_experiment["infra_region"],
            "TF_VAR_namespace": "armonik",
            "TF_VAR_prefix": "airflow-bench",
            "TF_VAR_project": "armonik-gcp-13469",
            "EXTRA_PARAMETERS_FILE": "/tmp/workdir/ArmoniK/extra.tfvars.json",
            "VERSIONS_FILE": "/tmp/workdir/ArmoniK/versions.tfvars.json",
            "PARAMETERS_FILE": "/tmp/workdir/ArmoniK/infrastructure/quick-deploy/gcp/parameters.tfvars.json",
        },
    )

    terraform_output = KubernetesPodOperator(
        task_id="terraform_output",
        name="terraform-output",
        image="hashicorp/terraform:1.8",
        cmds=["sh", "-c"],
        arguments=[
            "mkdir -p /airflow/xcom && terraform output -state=$(STATE_FILE) -json > /airflow/xcom/return.json"
        ],
        namespace="composer-user-workloads",
        volume_mounts=[k8s.V1VolumeMount(mount_path="/tmp/workdir", name="pvc-workdir-vol")],
        volumes=[
            k8s.V1Volume(
                name="pvc-workdir-vol",
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                    claim_name="pvc-workdir"
                ),
            )
        ],
        reattach_on_restart=True,
        do_xcom_push=True,
        on_finish_action="delete_succeeded_pod",
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id="kubernetes_default",
        full_pod_spec=k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        working_dir="/tmp/workdir/ArmoniK/infrastructure/quick-deploy/gcp",
                        name="terraform",
                    )
                ]
            )
        ),
        env_vars={
            "STATE_FILE": "armonik-terraform.tfstate",
            "TF_DATA_DIR": "/tmp/workdir/ArmoniK/infrastructure/quick-deploy/gcp/generated",
            "TF_PLUGIN_CACHE_DIR": "/tmp/workdir/ArmoniK/infrastructure/quick-deploy/gcp/generated/terraform-plugins",
        },
    )

    @task(multiple_outputs=True)
    def parse_terraform_output(outputs: dict[str, dict]) -> dict[str, str]:
        return {
            "cluster_name": outputs["gke"]["value"]["name"],
            "cluster_region": outputs["gke"]["value"]["region"],
            "armonik_control_plane_url": outputs["armonik"]["value"]["control_plane_url"],
        }

    parse_terraform_output = parse_terraform_output(terraform_output.output["return_value"])

    @task
    def get_kubeconfig(terraform_outputs):
        return generate_gke_kube_config(
            project_id="armonik-gcp-13469",
            cluster_name=terraform_outputs["gke"]["value"]["name"],
            cluster_location=terraform_outputs["gke"]["value"]["region"],
        )

    get_kubeconfig = get_kubeconfig(terraform_output.output["return_value"])

    update_kube_connection = UpdateAirflowConnectionOperator(
        task_id="update_kube_connection",
        conn_id="armonik_kubernetes_default",
        conn_type="kubernetes",
        description="Kubernetes connection for a remote ArmoniK cluster.",
        extra={"in_cluster": False, "kube_config": get_kubeconfig["return_value"]},
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

    run_client = GKEStartJobOperator(
        task_id="run_client",
        location=parse_terraform_output["cluster_region"],
        cluster_name=parse_terraform_output["cluster_name"],
        name="run-client",
        namespace="armonik",
        labels={"app": "armonik", "service": "run-client", "type": "others"},
        image=load_experiment["workload_image"],
        env_vars={
            "GrpcClient__Endpoint": parse_terraform_output["armonik_control_plane_url"],
            "HtcMock__NTasks": "10",
            "HtcMock__TotalCalculationTime": "00:00:00.0",
            "HtcMock__DataSize": "0",
            "HtcMock__MemorySize": "0",
            "HtcMock__SubTasksLevels": "2",
            "HtcMock__EnableUseLowMem": "false",
            "HtcMock__EnableSmallOutput": "false",
            "HtcMock__EnableFastCompute": "false",
            "HtcMock__Partition": "htcmock",
        },
        backoff_limit=1,
        completion_mode="NonIndexed",
        completions=1,
        parallelism=1,
        node_selector={"service": "others"},
        tolerations=[k8s.V1Toleration(effect="NoSchedule", key="service", value="others")],
        on_finish_action="delete_pod",
        log_pod_spec_on_failure=True,
        wait_until_job_complete=True,
    )

    terraform_destroy = KubernetesPodOperator(
        task_id="terraform_destroy",
        name="terraform-destroy",
        image="hashicorp/terraform:1.8",
        cmds=["terraform"],
        arguments=[
            "destroy",
            "-var-file=$(VERSIONS_FILE)",
            "-var-file=$(PARAMETERS_FILE)",
            "-var-file=$(EXTRA_PARAMETERS_FILE)",
            "-auto-approve",
        ],
        namespace="composer-user-workloads",
        volume_mounts=[k8s.V1VolumeMount(mount_path="/tmp/workdir", name="pvc-workdir-vol")],
        volumes=[
            k8s.V1Volume(
                name="pvc-workdir-vol",
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                    claim_name="pvc-workdir"
                ),
            )
        ],
        reattach_on_restart=True,
        on_finish_action="delete_succeeded_pod",
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id="kubernetes_default",
        full_pod_spec=k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        working_dir="/tmp/workdir/ArmoniK/infrastructure/quick-deploy/gcp",
                        name="terraform",
                    )
                ]
            )
        ),
        env_vars={
            "PREFIX": "airflow-bench",
            "TF_DATA_DIR": "/tmp/workdir/ArmoniK/infrastructure/quick-deploy/gcp/generated",
            "TF_PLUGIN_CACHE_DIR": "/tmp/workdir/ArmoniK/infrastructure/quick-deploy/gcp/generated/terraform-plugins",
            "TF_VAR_region": load_experiment["infra_region"],
            "TF_VAR_namespace": "armonik",
            "TF_VAR_prefix": "airflow-bench",
            "TF_VAR_project": "armonik-gcp-13469",
            "EXTRA_PARAMETERS_FILE": "/tmp/workdir/ArmoniK/extra.tfvars.json",
            "VERSIONS_FILE": "/tmp/workdir/ArmoniK/versions.tfvars.json",
            "PARAMETERS_FILE": "/tmp/workdir/ArmoniK/infrastructure/quick-deploy/gcp/parameters.tfvars.json",
        },
    )

    (
        load_experiment
        >> setup
        >> terraform_init
        >> terraform_apply
        >> terraform_output
        >> parse_terraform_output
        >> get_kubeconfig
        >> [update_kube_connection, update_armonik_connection]
        >> run_client
        >> terraform_destroy
    )


run_experiment()
