from datetime import datetime, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartJobOperator
from kubernetes.client import models as k8s


@dag(
    dag_id="run_experiment",
    description="Workflow for running a given workload from an existing client on a given infrastructure.",
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    max_active_runs=1,
)
def run_experiment():
    """Workflow assessing ArmoniK's performance against fault tolerance scenarios for the workload implemented by the ArmoniK HTC Mock client on a fixed infrastructure."""

    @task.kubernetes(
        task_id="setup",
        name="setup",
        image="python:3.11.5",
        namespace="composer-user-workloads",
        volume_mounts=[k8s.V1VolumeMount(mount_path="/tmp/workdir", name="pvc-workdir-vol")],
        volumes=[k8s.V1Volume(name="pvc-workdir-vol", persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="pvc-workdir"))],
        reattach_on_restart=True,
        on_finish_action="delete_succeeded_pod",
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id="kubernetes_default",
        env_vars={
            "SETUP_SCRIPT__REPO_REF": "fl/optional-upload",
            "SETUP_SCRIPT___AK_CONFIG": Path("/home/airflow/gcs/data/config.json").open().read(),
            "SETUP_SCRIPT__REPO_PATH": "/tmp/workdir/ArmoniK",
            "SETUP_SCRIPT__REPO_URL": "https://github.com/aneoconsulting/ArmoniK",
            "SETUP_SCRIPT___AK_ENVIRONMENT": "gcp",
        },
    )
    def setup():
        import json
        import logging
        import os
        import pathlib
        import shutil
        import subprocess
        import sys


        def clone_repo(repo_path: pathlib.Path, repo_url: str, ref: str) -> None:
            """Clone a Git repository from an URL and checkout to a specific commit, branch, or tag.
            If the destination path already exists, it will be overwritten.

            Parameters
            ----------
            repo_path : pathlib.Pathlib.Path
                Path where to clone the repository.
            repo_url : str
                URL of the Git repository to clone.
            ref : str
                Reference to checkout (commit hash, branch name, or tag name).

            Raises
            ------
            ValueError
                If the 'repo_path' provided corresponds to an existing file.
            git.exc.GitCommandError
                If an error occurs when cloning the repository or checking out.
            """
            if repo_path.exists():
                if repo_path.is_dir():
                    shutil.rmtree(repo_path)
                else:
                    raise ValueError(f"{repo_path} is not a path to a directory.")

            # Cone repository using Git CLI
            subprocess.run(["git", "clone", repo_url, str(repo_path)], check=True)

            # Checkout repository using Git CLI
            subprocess.run(["git", "checkout", ref], check=True, cwd=str(repo_path))


        def edit_default_parameters_file(
            repo_path: pathlib.Path, environment: str, config: dict
        ) -> None:
            """Edit the default parameters file for the Terraform ArmoniK deployment.

            This function takes a repository path, an environment name, and a dictionary
            of configuration parameters. It writes the configuration to a Terraform
            parameters file and renames it with a '.json' extension.

            Parameters
            ----------
            repo_path : pathlib.Path
                Path to a local clone of the repository hosting the ArmoniK project.
            environment : str
                Deployment environment ('locahost', 'aws', 'gcp').
            config : dict
                Configuration for deployment (the contents of the parameter file that will be written).


            Raises
            ------
            TypeError
                If the 'config' provided is not JSON-serializable.
            FileNotFoundError
                If the 'repo_path' doesn't correspond to a local clone of the ArmoniK repository.
            """
            parameters_file = (
                repo_path / f"infrastructure/quick-deploy/{environment}/parameters.tfvars"
            )

            with parameters_file.open("w") as file:
                file.write(json.dumps(config))

            parameters_file.rename(parameters_file.with_suffix(".tfvars.json"))


        def download_terraform_modules(repo_path: pathlib.Path, environment: str) -> None:
            """Download ArmoniK Terraform modules based on the configuration provided in the 'versions'
            file of ArmoniK repository already cloned.

                Parameters
            ----------
            repo_path : pathlib.Pathlib.Path
                Path where to download the modules.
            environment : str
                Deployment environment ('locahost', 'aws', 'gcp').

            """
            with (repo_path / "versions.tfvars.json").open() as file:
                data = json.loads(file.read())
                modules_source = data["armonik_images"]["infra"][0]
                modules_version = data["armonik_versions"]["infra"]

            generated_dir_path = (
                repo_path / f"infrastructure/quick-deploy/{environment}/generated"
            )
            generated_dir_path.mkdir(exist_ok=False)

            clone_repo(
                repo_path=generated_dir_path / "infra-modules",
                repo_url=modules_source,
                ref=modules_version,
            )

        LOG = logging.getLogger(__name__)
        LOG.setLevel(os.environ.get("SETUP_SCRIPT__LOGGING_LEVEL", logging.INFO))

        try:
            repo_path = pathlib.Path(os.environ["SETUP_SCRIPT__REPO_PATH"])
            repo_url = os.environ["SETUP_SCRIPT__REPO_URL"]
            repo_ref = os.environ["SETUP_SCRIPT__REPO_REF"]
            environment = os.environ["SETUP_SCRIPT___AK_ENVIRONMENT"]
            config = json.loads(os.environ["SETUP_SCRIPT___AK_CONFIG"])

            if environment not in ["localhost", "gcp"]:
                raise ValueError(f"Deployment environment {environment} not supported.")

        except KeyError as error:
            LOG.error(f"Missing environment variable: {error.args[0]}.")
            sys.exit(1)
        except json.decoder.JSONDecodeError as error:
            LOG.error(
                f"The configuration format is invalid. Error during parsing: {error.msg.lower()}."
            )
            sys.exit(1)
        except ValueError as error:
            LOG.error(error.msg)

        LOG.info(f"Cloning repository {repo_url} in {repo_path}.")
        clone_repo(repo_path=repo_path, repo_url=repo_url, ref=repo_ref)
        LOG.info(f"Editing default parameters file for {environment} deployment.")
        edit_default_parameters_file(
            repo_path=repo_path, environment=environment, config=config
        )
        LOG.info("Downloading ArmoniK's Terraform modules...")
        download_terraform_modules(repo_path=repo_path, environment=environment)

        sys.exit(0)


    setup = setup()


    terraform_init = KubernetesPodOperator(
        task_id="terraform_init",
        name="terraform-init",
        image="hashicorp/terraform:1.8",
        cmds=["terraform"],
        arguments=["init", "-upgrade", "-reconfigure", "-backend-config=bucket=$(PREFIX)-tfstate"],
        namespace="composer-user-workloads",
        volume_mounts=[k8s.V1VolumeMount(mount_path="/tmp/workdir", name="pvc-workdir-vol")],
        volumes=[k8s.V1Volume(name="pvc-workdir-vol", persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="pvc-workdir"))],
        reattach_on_restart=True,
        on_finish_action="delete_succeeded_pod",
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id="kubernetes_default",
        full_pod_spec=k8s.V1Pod(spec=k8s.V1PodSpec(containers=[k8s.V1Container(working_dir="/tmp/workdir/ArmoniK/infrastructure/quick-deploy/gcp", name="terraform")])),
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
        arguments=["apply", "-var-file=$(VERSIONS_FILE)", "-var-file=$(PARAMETERS_FILE)", "-var-file=$(EXTRA_PARAMETERS_FILE)", "-auto-approve"],
        namespace="composer-user-workloads",
        volume_mounts=[k8s.V1VolumeMount(mount_path="/tmp/workdir", name="pvc-workdir-vol")],
        volumes=[k8s.V1Volume(name="pvc-workdir-vol", persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="pvc-workdir"))],
        reattach_on_restart=True,
        on_finish_action="delete_succeeded_pod",
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id="kubernetes_default",
        full_pod_spec=k8s.V1Pod(spec=k8s.V1PodSpec(containers=[k8s.V1Container(working_dir="/tmp/workdir/ArmoniK/infrastructure/quick-deploy/gcp", name="terraform")])),
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
        arguments=["mkdir -p /airflow/xcom && terraform output -state=$(STATE_FILE) -json > /airflow/xcom/return.json"],
        namespace="composer-user-workloads",
        volume_mounts=[k8s.V1VolumeMount(mount_path="/tmp/workdir", name="pvc-workdir-vol")],
        volumes=[k8s.V1Volume(name="pvc-workdir-vol", persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="pvc-workdir"))],
        reattach_on_restart=True,
        do_xcom_push=True,
        on_finish_action="delete_succeeded_pod",
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id="kubernetes_default",
        full_pod_spec=k8s.V1Pod(spec=k8s.V1PodSpec(containers=[k8s.V1Container(working_dir="/tmp/workdir/ArmoniK/infrastructure/quick-deploy/gcp", name="terraform")])),
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
        project_id="armonik-gcp-13469",
        location=parse_terraform_output["cluster_region"],
        cluster_name=parse_terraform_output["cluster_name"],
        name="run-client",
        namespace="armonik",
        labels={"app": "armonik", "service": "run-client", "type": "others"},
        image="dockerhubaneo/armonik_core_htcmock_test_client:0.23.1",
        env_vars={
            "GrpcClient__Endpoint": parse_terraform_output["armonik_control_plane_url"],
            "HtcMock__NTasks": "2000",
            "HtcMock__TotalCalculationTime": "00:00:50.0",
            "HtcMock__DataSize": "50",
            "HtcMock__MemorySize": "50",
            "HtcMock__SubTasksLevels": "100",
            "HtcMock__EnableUseLowMem": "false",
            "HtcMock__EnableSmallOutput": "false",
            "HtcMock__EnableFastCompute": "false",
            "HtcMock__Partition": "htcmock",
        },
        backoff_limit=1,
        completion_mode="NonIndexed",
        completions=1,
        parallelism=1,
        node_selector = {"service": "others"},
        tolerations = [k8s.V1Toleration(effect="NoSchedule", key="service", value="others")],
        on_finish_action="delete_pod",
        wait_until_job_complete=True,
    )

    terraform_destroy = KubernetesPodOperator(
        task_id="terraform_destroy",
        name="terraform-destroy",
        image="hashicorp/terraform:1.8",
        cmds=["terraform"],
        arguments=["destroy", "-var-file=$(VERSIONS_FILE)", "-var-file=$(PARAMETERS_FILE)", "-var-file=$(EXTRA_PARAMETERS_FILE)", "-auto-approve"],
        namespace="composer-user-workloads",
        volume_mounts=[k8s.V1VolumeMount(mount_path="/tmp/workdir", name="pvc-workdir-vol")],
        volumes=[k8s.V1Volume(name="pvc-workdir-vol", persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="pvc-workdir"))],
        reattach_on_restart=True,
        on_finish_action="delete_succeeded_pod",
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id="kubernetes_default",
        full_pod_spec=k8s.V1Pod(spec=k8s.V1PodSpec(containers=[k8s.V1Container(working_dir="/tmp/workdir/ArmoniK/infrastructure/quick-deploy/gcp", name="terraform")])),
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

    setup >> terraform_init >> terraform_apply >> terraform_output >> run_client >> terraform_destroy

    # KubernetesPodOperator(
    #     task_id="setup",
    #     name="setup",
    #     image="python:3.11.5",
    #     cmds=["python", "-c"],
    #     arguments=["print('Hello')"],
    #     namespace="composer-user-workloads",
    #     volume_mounts=[k8s.V1VolumeMount(mount_path="/tmp/workdir", name="pvc-workdir-vol")],
    #     volumes=[k8s.V1Volume(name="pvc-workdir-vol", persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="pvc-workdir"))],
    #     reattach_on_restart=True,
    #     on_finish_action="delete_succeeded_pod",
    #     config_file="/home/airflow/composer_kube_config",
    #     kubernetes_conn_id="kubernetes_default",
    #     env_vars={
    #         "SETUP_SCRIPT__REPO_REF": "main",
    #         "SETUP_SCRIPT___AK_CONFIG": "{\"test\": \"ok\"}",
    #         "SETUP_SCRIPT__REPO_PATH": "/tmp/workdir/ArmoniK",
    #         "SETUP_SCRIPT__REPO_URL": "https://github.com/aneoconsulting/ArmoniK",
    #         "SETUP_SCRIPT___AK_ENVIRONMENT": "gcp",
    #     },
    # )


run_experiment()
