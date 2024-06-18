import json
import re

from pathlib import Path
from typing import Any, Sequence

import boto3
import google.auth
import yaml

from airflow.models.baseoperator import BaseOperator
from airflow.models.connection import Connection
from airflow.utils.context import Context
from airflow.utils.session import create_session
from google.cloud.container_v1 import ClusterManagerClient
from sqlalchemy import select


class UpdateAirflowConnectionOperator(BaseOperator):
    """
    Create or update a connection in the Airflow database.

    Args:
        conn_id (str): The connection ID.
        conn_type (str): The connection type.
        description (str): The connection description.
        host (str): The host.
        login (str): The login.
        password (str): The password.
        schema (str): The schema.
        port (int): The port number.
        extra (str): Extra metadata. Non-standard data such as private/SSH keys can be saved here. JSON
            encoded object.
        uri (str): URI address describing connection parameters.
    """

    template_fields: Sequence[str] = (
        "conn_id",
        "conn_type",
        "description",
        "host",
        "login",
        "password",
        "schema",
        "port",
        "extra",
        "uri",
    )

    def __init__(
        self,
        conn_id: str | None = None,
        conn_type: str | None = None,
        description: str | None = None,
        host: str | None = None,
        login: str | None = None,
        password: str | None = None,
        schema: str | None = None,
        port: int | None = None,
        extra: str | dict | None = None,
        uri: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.conn_type = conn_type
        self.description = description
        self.host = host
        self.login = login
        self.password = password
        self.schema = schema
        self.port = port
        self.extra = extra
        self.uri = uri

    def execute(self, context: Context) -> Any:
        with create_session() as session:
            self.log.info(f"connection id: {self.conn_id}")
            conn: Connection = session.scalar(
                select(Connection).where(Connection.conn_id == self.conn_id)
            )
            if conn:
                self.log.info("connection already exists")
                session.delete(conn)
                session.commit()
                self.log.info("already existing connection deleted")
            conn = Connection(
                conn_id=self.conn_id,
                conn_type=self.conn_type,
                description=self.description,
                host=self.host,
                login=self.login,
                password=self.password,
                schema=self.schema,
                port=self.port,
                extra=self.extra,
                uri=self.uri,
            )
            session.add(conn)
            session.commit()
            self.log.info(f"created new connection: {conn}")


class UpdateArmoniKClusterConnectionOperator(UpdateAirflowConnectionOperator):
    """
    Create or update the Airflow connection related to the ArmoniK Kubernetes cluster.

    Args:
        cluster_arn (str): The Kubernetes cluster ARN.
        description (str): The connection description.
        conn_id (str, optional): The connection ID. Default is 'armonik_kubernetes_default'.
    """

    def __init__(self, cluster_arn: str, description: str, conn_id: str = "armonik_kubernetes_default", **kwargs) -> None:
        super().__init__(
            conn_id=conn_id,
            conn_type="kubernetes",
            description=description,
            **kwargs,
        )
        self.cluster_arn = cluster_arn

    def execute(self, context: Context) -> None:
        gke_pattern = re.compile(r'^projects/(?P<project_id>[^/]+)/locations/(?P<cluster_location>[^/]+)/clusters/(?P<cluster_name>[^/]+)$')
        eks_pattern = re.compile(r'^aws:(?P<project_id>[^/]+)/locations/(?P<cluster_location>[^/]+)/clusters/(?P<cluster_name>[^/]+)$')

        if gke_pattern.match(self.cluster_arn):
            kube_config = self.generate_gke_kube_config(**gke_pattern.match(self.cluster_arn).groupdict())
        elif eks_pattern.match(self.cluster_arn):
            kube_config = self.generate_eks_kube_config(**eks_pattern.match(self.cluster_arn).groupdict())
        else:
            kube_config = self.load_local_kube_config(Path.cwd() / "composer_kube_config")

        self.extra = {
            "in_cluster": False,
            "kube_config": json.dumps(kube_config),
        }
        return super().execute(context)

    @staticmethod
    def generate_gke_kube_config(project_id: str, cluster_name: str, cluster_location: str) -> dict[str, Any]:
        """Generates Kubernetes configuration for a Google Kubernetes Engine (GKE) cluster.

        Args:
            project_id (str): The ID of the GCP project that the GKE cluster belongs to.
            cluster_name (str): The name of the GKE cluster.
            cluster_location (str): The location of the GKE cluster (e.g., 'us-central1').

        Returns:
            dict: Kubernetes configuration in dict format.

        Raises:
            google.auth.exceptions.DefaultCredentialsError: If the default credentials cannot be retrieved.
            google.api_core.exceptions.GoogleAPICallError: If an error occurs when calling the GCP API.

        Note:
            This function requires appropriate permissions to access GCP resources.

        """
        # Get Application Default Credentials
        credentials, _ = google.auth.default()

        # Get the cluster config from GCP
        cluster_manager_client = ClusterManagerClient(credentials=credentials)

        cluster = cluster_manager_client.get_cluster(
            name=f"projects/{project_id}/locations/{cluster_location}/clusters/{cluster_name}"
        )

        endpoint = f"https://{cluster.endpoint}"
        cert = cluster.master_auth.cluster_ca_certificate

        return _render_kube_config(
            cluster_name=cluster_name,
            cluster_endpoint=endpoint,
            cluster_cert=cert,
            cluster_user_exec={
                    "apiVersion": "client.authentication.k8s.io/v1beta1",
                    "command": "gke-gcloud-auth-plugin",
                    "installHint": "Install gke-gcloud-auth-plugin for use with kubectl by following https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke",
                    "provideClusterInfo": True,
                }
            )

    @staticmethod
    def generate_eks_kube_config(cluster_name: str, cluster_region: str) -> dict[str, Any]:
        """Generates Kubernetes configuration for an Amazon Elastic Kubernetes Service (EKS) cluster.

        Args:
            cluster_name (str): The name of the EKS cluster.
            cluster_region (str): The region where the EKS cluster is located.

        Returns:
            dict[str, Any]: Kubernetes configuration in dict format.

        """        
        session = boto3.Session(region_name=cluster_region)
        eks_client = session.client("eks")

        cluster = eks_client.describe_cluster(name=cluster_name)
        endpoint = cluster["cluster"]["endpoint"]
        cert = cluster["cluster"]["certificateAuthority"]["data"]

        return _render_kube_config(
            cluster_name=cluster_name,
            cluster_endpoint=endpoint,
            cluster_cert=cert,
            cluster_user_exec={
                "apiVersion": "client.authentication.k8s.io/v1alpha1",
                "command": "heptio-authenticator-aws",
                "args": [
                    "token", "-i", cluster_name
                ]
            }
        )

    @staticmethod
    def load_local_kube_config(path: Path) -> dict[str, Any]:
        """Loads local Kubernetes configuration from a specified file path.

        Args:
            path (Path): The file path to the local Kubernetes configuration file.

        Returns:
            dict[str, Any]: Kubernetes configuration in dict format.

        """
        with Path(path).open() as f:
            return yaml.safe_load(f.read())


def _render_kube_config(cluster_name: str, cluster_endpoint: str, cluster_cert: str, cluster_user_exec) -> dict[str, Any]:
    """Renders a Kubernetes configuration based on cluster's metadata.

    Args:
        cluster_name (str): The name of the cluster.
        cluster_endpoint (str): The endpoint URL of the cluster.
        cluster_cert (str): The certificate authority data for the cluster.
        cluster_user_exec: The user exec configuration for authentication.

    Returns:
        dict[str, Any]: Kubernetes configuration in dict format.

    """
    return {
        "apiVersion": "v1",
        "kind": "Config",
        "clusters": [
            {
                "name": cluster_name,
                "cluster": {
                    "certificate-authority-data": cluster_cert,
                    "server": cluster_endpoint,
                },
            }
        ],
        "contexts": [
            {
                "name": "main",
                "context": {
                    "cluster": cluster_name,
                    "user": "armonik-bench",
                },
            }
        ],
        "current-context": "main",
        "preferences": {},
        "users": [
            {
                "name": "armonik-bench",
                "user": {
                    "exec": cluster_user_exec
                },
            }
        ],
    }
