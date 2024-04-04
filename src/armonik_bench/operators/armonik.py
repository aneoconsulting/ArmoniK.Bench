import json

from pathlib import Path
from typing import Sequence

import yaml

from airflow import settings
from airflow.exceptions import AirflowNotFoundException, AirflowException
from airflow.models.connection import Connection
from airflow.operators.bash import BashOperator
from airflow.utils.context import Context
from airflow.providers.github.hooks.github import GithubHook
from github import GithubException, Repository, UnknownObjectException


class BaseArmoniKClusterOperator(BashOperator):
    template_fields: Sequence[str] = (
        "release",
        "environment",
        "region",
        "config",
        "armonik_conn_id",
        "github_conn_id",
        "bucket_prefix",
    )
    __armonik_github_repo_full_name = "aneoconsulting/ArmoniK"

    def __init__(
        self,
        *,
        release: str,
        environment: str,
        region: str,
        config: dict | str,
        armonik_conn_id: str,
        kubernetes_conn_id: str,
        github_conn_id: str,
        bucket_prefix: str,
        **kwargs,
    ) -> None:
        self.release = release
        self.environment = environment
        self.region = region
        self.config = config
        self.armonik_conn_id = armonik_conn_id
        self.kubernetes_conn_id = kubernetes_conn_id
        self.github_conn_id = github_conn_id
        self.bucket_prefix = bucket_prefix
        super().__init__(
            bash_command="",
            append_env=True,
            cwd=str(Path.cwd()),
            **kwargs,
        )

    def check_release(self):
        try:
            self.log.info(f"Using Github {self.github_conn_id} connection.")
            hook = GithubHook(github_conn_id=self.github_conn_id)
            resource = hook.client

            repo: Repository = resource.get_repo(
                full_name_or_id=self.__armonik_github_repo_full_name
            )

            match self.release:
                case "main":
                    release = "main"
                case "latest":
                    release = repo.get_latest_release().tag_name
                case _:
                    try:
                        release = repo.get_release(self.release).tag_name
                    except UnknownObjectException:
                        release = repo.get_branch(self.release).name
            self.log.info(f"Release is {release}.")
            self.release = release

        except GithubException as github_error:
            raise AirflowException(f"Failed to execute GithubOperator, error: {github_error}")
        except Exception as e:
            raise AirflowException(f"GitHub operator error: {e}")

    def clone_repo(self, context: Context):
        self.env = {
            "PREFIX": self.bucket_prefix,
            "REGION": self.region,
            "PARAMETERS_FILE": "parameters.tfvars.json",
        }
        self.bash_command = f"""
            if [ ! -d ArmoniK ] ; then
                git clone --depth 1 -b {self.release} https://github.com/aneoconsulting/ArmoniK.git
            fi
            """
        super().execute(context)

    def replace_default_parameters_file(self):
        # Delete default parameters file
        Path(
            Path(self.cwd)
            / f"ArmoniK/infrastructure/quick-deploy/{self.environment}/all-in-one/parameters.tfvars"
        ).unlink(missing_ok=True)
        Path(
            Path(self.cwd)
            / f"ArmoniK/infrastructure/quick-deploy/{self.environment}/all-in-one/parameters.tfvars.json"
        ).unlink(missing_ok=True)
        with (
            Path(self.cwd)
            / f"ArmoniK/infrastructure/quick-deploy/{self.environment}/all-in-one/parameters.tfvars.json"
        ).open("w") as file:
            file.write(json.dumps(self.config))

    def clean_up(self, context: Context):
        self.bash_command = "rm -rf ArmoniK"
        super().execute(context)


class ArmoniKDeployClusterOperator(BaseArmoniKClusterOperator):
    template_fields: Sequence[str] = (
        "release",
        "environment",
        "region",
        "config",
        "armonik_conn_id",
        "kubernetes_conn_id",
        "github_conn_id",
        "bucket_prefix",
    )

    def __init__(
        self,
        *,
        release: str,
        environment: str,
        config: dict | str,
        region: str = "europ-west1",
        armonik_conn_id: str = "armonik_default",
        kubernetes_conn_id: str = "kubernetes_default",
        github_conn_id: str = "github_default",
        bucket_prefix: str = "airflow-bench",
        **kwargs,
    ) -> None:
        self.release = release
        self.environment = environment
        self.region = region
        self.config = config
        self.armonik_conn_id = armonik_conn_id
        self.kubernetes_conn_id = kubernetes_conn_id
        self.github_conn_id = github_conn_id
        self.bucket_prefix = bucket_prefix
        super().__init__(
            release=release,
            environment=environment,
            region=region,
            config=config,
            armonik_conn_id=armonik_conn_id,
            kubernetes_conn_id=kubernetes_conn_id,
            github_conn_id=github_conn_id,
            bucket_prefix=bucket_prefix,
            **kwargs,
        )

    def deploy(self, context: Context):
        self.bash_command = (
            f"cd ./ArmoniK/infrastructure/quick-deploy/{self.environment}/all-in-one/ && make"
        )
        super().execute(context)

    def _create_or_update_connection(self, conn_id: str):
        try:
            conn = Connection.get_connection_from_secrets(conn_id=conn_id)
            self.log.info(f"Connection {conn_id} alread exists and will be overwritten.")
        except AirflowNotFoundException:
            conn = Connection(conn_id=conn_id)
            self.log.info(f"Connection {conn_id} created.")
        return conn

    def set_armonik_connection(self):
        output_path = (
            Path(self.cwd)
            / f"ArmoniK/infrastructure/quick-deploy/{self.environment}/all-in-one/generated/armonik-output.json"
        )
        self.log.info(f"Reading outputs from {output_path}")
        with output_path.open() as file:
            outputs = json.loads(file.read())
            url = outputs["armonik"]["control_plane_url"].removeprefix("http://")
            if ":" in url:
                host = url.split(":")[0]
                port = url.split(":")[1]
            else:
                host = url
                port = None
        self.log.info(f"Get host {host} and port {port} from armonik-outputs.json.")

        session = settings.Session()
        conn = self._create_or_update_connection(self.armonik_conn_id)
        conn.conn_type = "grpc"
        conn.host = host
        conn.port = port
        conn.description = "gRPC connection for a remote ArmoniK cluster"
        conn.extra = json.dumps({"auth_type": "NO_AUTH"})
        session.add(conn)
        session.commit()
        self.log.info(f"Connection {self.armonik_conn_id} added to database.")

    def set_kubernetes_connection(self):
        # Get kubeconfig file
        if self.environment == "localhost":
            kube_config_file = Path.home() / ".kube/config"
        else:
            kube_config_file = (
                Path(self.cwd)
                / f"ArmoniK/infrastructure/quick-deploy/{self.environment}/generated/kubeconfig"
            )
        with kube_config_file.open() as file:
            kube_config = yaml.safe_load(file.read())

        self.log.info(f"kube_config: {kube_config}")
        session = settings.Session()
        conn = self._create_or_update_connection(self.kubernetes_conn_id)
        conn.conn_type = "kubernetes"
        conn.description = "Kubernetes connection for a remote ArmoniK cluster"
        conn.extra = json.dumps(
            {
                "in_cluster": False,
                "kube_config": json.dumps(kube_config),
            }
        )
        session.add(conn)
        session.commit()
        self.log.info(Connection.get_connection_from_secrets(conn_id=self.kubernetes_conn_id))
        self.log.info(f"Connection {self.kubernetes_conn_id} added to database.")

    def execute(self, context: Context):
        self.check_release()
        self.clone_repo(context)
        self.replace_default_parameters_file()
        self.deploy(context)
        self.set_armonik_connection()
        self.set_kubernetes_connection()
        # self.clean_up(context)


class ArmoniKDestroyClusterOperator(BaseArmoniKClusterOperator):
    template_fields: Sequence[str] = (
        "release",
        "environment",
        "region",
        "config",
        "armonik_conn_id",
        "kubernetes_conn_id",
        "github_conn_id",
        "bucket_prefix",
    )

    def __init__(
        self,
        *,
        release: str,
        environment: str,
        config: str,
        region: str = "europ-west1",
        armonik_conn_id: str = "armonik_default",
        kubernetes_conn_id: str = "kubernetes_default",
        github_conn_id: str = "github_default",
        bucket_prefix: str = "airflow-bench",
        **kwargs,
    ) -> None:
        self.release = release
        self.environment = environment
        self.region = region
        self.config = config
        self.armonik_conn_id = armonik_conn_id
        self.kubernetes_conn_id = kubernetes_conn_id
        self.github_conn_id = github_conn_id
        self.bucket_prefix = bucket_prefix
        super().__init__(
            release=release,
            environment=environment,
            region=region,
            config=config,
            armonik_conn_id=armonik_conn_id,
            kubernetes_conn_id=kubernetes_conn_id,
            github_conn_id=github_conn_id,
            bucket_prefix=bucket_prefix,
            **kwargs,
        )

    def destroy(self, context: Context):
        self.bash_command = f"cd ./ArmoniK/infrastructure/quick-deploy/{self.environment}/all-in-one/ && make get-modules && make destroy"
        super().execute(context)

    def _remove_connection(self, conn_id: str) -> None:
        session = settings.Session()
        try:
            conn = Connection.get_connection_from_secrets(conn_id=conn_id)
            self.log.info(f"Connection {conn_id} exists and will be deleted.")
            session.delete(conn)
            session.commit()
            self.log.info(f"Connection {conn_id} removed from database.")
        except AirflowNotFoundException:
            self.log.info(f"Connection {conn_id} doesn't exiss.")

    def remove_armonik_connection(self) -> None:
        self._remove_connection(self.armonik_conn_id)

    def remove_kubernetes_connection(self) -> None:
        self._remove_connection(self.kubernetes_conn_id)

    def execute(self, context: Context):
        super().check_release()
        super().clone_repo(context)
        super().replace_default_parameters_file()
        self.destroy(context)
        self.remove_armonik_connection()
        self.remove_kubernetes_connection()
        # super().clean_up(context)
