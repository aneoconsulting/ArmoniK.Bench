import re

from kubernetes.client import CoreV1Api


def kubernetes_n_nodes_ready(
    core_v1_client: CoreV1Api, n: int, node_name_pattern: str = ""
) -> bool:
    """Returns True if the number of 'Ready' nodes in the given Kubernetes cluster matches the desired number, False if not.
    A filter can be applied to the node names to consider only a subset of the nodes.

    Parameters
    ----------
    core_v1_client : kubernetes.CoreV1Api
        Kubernetes CoreV1 API client connected to a Kubernetes cluster.
    n : int
        The number of desired nodes.
    node_name_pattern : str
        RegEx to apply on node names. Default is "".

    Returns
    -------
    bool
        True if the number of nodes matches the desired number, False if not.
    """
    ready_nodes = 0
    for node in core_v1_client.list_node().items:
        if (
            re.match(node_name_pattern, node.metadata.name)
            and bool(node.status.conditions[-1].status)
            and node.status.conditions[-1].type == "Ready"
        ):
            ready_nodes += 1
        # import pdb; pdb.set_trace()
    if n == ready_nodes:
        return True
    return False
