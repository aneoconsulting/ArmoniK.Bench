from airflow.providers.google.cloud.hooks.compute import ComputeEngineHook
from google.cloud.compute_v1 import InstancesClient, ZonesClient


def list_zones(project: str, region: str, gcp_conn_id: str = "google_cloud_default") -> list[str]:
    """Get the list of available zones within a given GCP region.

    Args:
        project (str): ID of the GCP project in use.
        region (str): Name of the GCP region.
        gcp_conn_id: ID of the connection to Google Cloud Default is 'google_cloud_default'.

    Return:
        list[str]: The list of the zones within the region. If the region doesn't exist or is not available
        within the project, the returned list is empty.
    """
    zones = []
    zones_client = ZonesClient(
        credentials=ComputeEngineHook(api_version="v1", gcp_conn_id=gcp_conn_id).get_credentials()
    )
    for zone in zones_client.list(project=project):
        if zone.region.split("regions/")[1] == region:
            zones.append(zone.name)
    return zones


def list_instances_from_node_pool(
    project: str,
    region: str,
    cluster_name: str,
    node_pool: str,
    provide_zone: bool = True,
    gcp_conn_id: str = "google_cloud_default",
) -> list[str | dict[str, str]]:
    """List instances within a given node pool of a container cluster in GCP.

    Args:
        project (str): ID of the project in use.
        region (str): GCP region of the cluster.
        cluster_name (str): Name of the container cluster.
        node_pool (str): Name of the node pool.
        provide_zone (bool): Wheither to return the zone where the instance is deployed. Default is True.
        gcp_conn_id: ID of the connection to Google Cloud Default is 'google_cloud_default'.

    Return:
        list[str | dict[str, str]]: The list of the instance names within the cluster's node pool. If the
            cluster or the node pool don't exist within the project, the returned list is empty. If
            'provide_zone' is True, the list returned contains the zone associated to each instance name.
    """
    instances = []
    instances_client = InstancesClient(
        credentials=ComputeEngineHook(api_version="v1", gcp_conn_id=gcp_conn_id).get_credentials()
    )
    for zone in list_zones(project=project, region=region, gcp_conn_id=gcp_conn_id):
        for instance in instances_client.list(project=project, zone=zone):
            metadata = {item.key: item.value for item in instance.metadata.items}
            if metadata["cluster_name"] == cluster_name and metadata["node_pool"] == node_pool:
                instances.append(
                    instance.name
                    if not provide_zone
                    else {"resource_id": instance.name, "zone": zone}
                )
    return instances
