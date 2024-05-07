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

import os

from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartJobOperator
from kubernetes.client import models as k8s


@dag(
    dag_id="run_experiment",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    # UI parameters
    description="Carry out a benchmark experiment on a given ArmoniK application and deployment",
    doc_md=__doc__,
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
    params={},
    dagrun_timeout=timedelta(
        minutes=int(os.environ.get("DAG__RUN_EXPERIMENT__DAGRUN_TIMEMOUT", 300))
    ),
)
def run_experiment():
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
            "SETUP_SCRIPT__REPO_REF": "fl/optional-upload",
            "SETUP_SCRIPT__AK_CONFIG": Path("/home/airflow/gcs/data/config.json").open().read(),
            "SETUP_SCRIPT__REPO_PATH": "/tmp/workdir/ArmoniK",
            "SETUP_SCRIPT__REPO_URL": "https://github.com/aneoconsulting/ArmoniK",
            "SETUP_SCRIPT__AK_ENVIRONMENT": "gcp",
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
            "TF_VAR_region": "us-central1",
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
            "TF_VAR_region": "us-central1",
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

    run_client = GKEStartJobOperator(
        task_id="run_client",
        location=parse_terraform_output["cluster_region"],
        cluster_name=parse_terraform_output["cluster_name"],
        name="run-client",
        namespace="armonik",
        labels={"app": "armonik", "service": "run-client", "type": "others"},
        image="dockerhubaneo/armonik_core_htcmock_test_client:0.23.1",
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
            "TF_VAR_region": "us-central1",
            "TF_VAR_namespace": "armonik",
            "TF_VAR_prefix": "airflow-bench",
            "TF_VAR_project": "armonik-gcp-13469",
            "EXTRA_PARAMETERS_FILE": "/tmp/workdir/ArmoniK/extra.tfvars.json",
            "VERSIONS_FILE": "/tmp/workdir/ArmoniK/versions.tfvars.json",
            "PARAMETERS_FILE": "/tmp/workdir/ArmoniK/infrastructure/quick-deploy/gcp/parameters.tfvars.json",
        },
    )

    (
        setup
        >> terraform_init
        >> terraform_apply
        >> terraform_output
        >> parse_terraform_output
        >> run_client
        >> terraform_destroy
    )


run_experiment()
