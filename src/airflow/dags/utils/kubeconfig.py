import google.auth

from google.cloud.container_v1 import ClusterManagerClient


def generate_gke_kube_config(project_id: str, cluster_name: str, cluster_location: str) -> dict:
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

    server = cluster.endpoint
    cert = cluster.master_auth.cluster_ca_certificate

    return {
        "apiVersion": "v1",
        "kind": "Config",
        "clusters": [
            {
                "name": cluster_name,
                "cluster": {
                    "certificate-authority-data": cert,
                    "server": f"https://{server}",
                },
            }
        ],
        "contexts": [
            {
                "name": cluster_name,
                "context": {
                    "cluster": cluster_name,
                    "user": cluster_name,
                },
            }
        ],
        "current-context": cluster_name,
        "preferences": {},
        "users": [
            {
                "name": cluster_name,
                "user": {
                    "exec": {
                        "apiVersion": "client.authentication.k8s.io/v1beta1",
                        "command": "gke-gcloud-auth-plugin",
                        "installHint": "Install gke-gcloud-auth-plugin for use with kubectl by following https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke",
                        "provideClusterInfo": True,
                    }
                },
            }
        ],
    }
