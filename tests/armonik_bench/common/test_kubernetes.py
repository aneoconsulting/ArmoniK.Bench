import pytest

from kubernetes.client import CoreV1Api
from kubernetes.client.models import (
    v1_object_meta,
    v1_node,
    v1_node_condition,
    v1_node_list,
    v1_node_status,
)

from armonik_bench.common.kube import kubernetes_n_nodes_ready


@pytest.mark.parametrize(
    ("n", "pattern", "expected_output"),
    [
        (3, "", True),
        (1, ".*worker.*", False),
        (2, ".*worker.*", True),
        (3, ".*worker.*", False),
        (1, ".*woker.*", False),
    ],
)
def test_kubernetes_n_nodes_ready(mocker, n: int, pattern: str, expected_output: bool) -> None:
    client = CoreV1Api()
    mocker.patch.object(
        CoreV1Api,
        "list_node",
        return_value=v1_node_list.V1NodeList(
            items=[
                v1_node.V1Node(
                    metadata=v1_object_meta.V1ObjectMeta(name=node["name"]),
                    status=v1_node_status.V1NodeStatus(
                        conditions=[
                            v1_node_condition.V1NodeCondition(
                                type=node["type"], status=node["status"]
                            )
                        ]
                    ),
                )
                for node in [
                    {"name": "gke-database-0", "type": "Ready", "status": True},
                    {"name": "gke-worker-0", "type": "Ready", "status": True},
                    {"name": "gke-worker-1", "type": "Ready", "status": True},
                    {"name": "gke-worker-2", "type": "NotReady", "status": False},
                ]
            ]
        ),
    )
    assert kubernetes_n_nodes_ready(client, n, pattern) == expected_output
