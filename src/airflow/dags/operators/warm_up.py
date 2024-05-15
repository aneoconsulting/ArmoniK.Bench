import re

from datetime import timedelta
from typing import Sequence

from airflow.sensors.base import BaseSensorOperator
from airflow.providers.grpc.hooks.grpc import GrpcHook
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.utils.context import Context
from armonik.client import ArmoniKHealthChecks
from armonik.common import ServiceHealthCheckStatus
from grpc import RpcError


class KubernetesNodesReadySensor(BaseSensorOperator):
    """
    Sensor that checks if a certain number of Kubernetes nodes are ready.

    Args:
        n_nodes (int): Number of nodes to check for readiness.
        poke_interval (timedelta): Time interval between consecutive pokes.
        timeout (timedelta): Timeout duration for the sensor.
        node_name_pattern (str, optional): Regular expression pattern for matching node names.
            Default is '.*'.
        kubernetes_conn_id (str, optional): Connection ID for Kubernetes. Default is
            'kubernetes_default'.
    """

    template_fields: Sequence[str] = tuple({"n_nodes"})

    def __init__(
        self,
        n_nodes: int,
        poke_interval: timedelta,
        timeout: timedelta,
        node_name_pattern: str = ".*",
        kubernetes_conn_id: str = "kubernetes_default",
        **kwargs,
    ) -> None:
        super().__init__(
            poke_interval=poke_interval, timeout=timeout, soft_fail=False, mode="poke", **kwargs
        )
        self.n_nodes = n_nodes
        self.node_name_pattern = re.compile(node_name_pattern)
        self.kubernetes_conn_id = kubernetes_conn_id

    def kubernetes_n_nodes_ready(self) -> bool:
        """
        Checks if the required number of nodes are ready in the Kubernetes cluster.

        Returns:
            bool: True if the required number of nodes are ready, False otherwise.
        """
        ready_nodes = 0
        core_v1_client = KubernetesHook(self.kubernetes_conn_id).core_v1_client
        for node in core_v1_client.list_node().items:
            if (
                re.fullmatch(self.node_name_pattern, node.metadata.name)
                and bool(node.status.conditions[-1].status)
                and node.status.conditions[-1].type == "Ready"
            ):
                ready_nodes += 1
        if self.n_nodes == ready_nodes:
            self.log.info("All the nodes are ready.")
            return True
        self.log.info(f"Waiting for {self.n_nodes - ready_nodes} more nodes to be ready...")
        return False

    def poke(self, context: Context) -> bool:
        """
        Performs a poke to check if the required number of Kubernetes nodes are ready.

        Args:
            context (Context): Airflow task context.

        Returns:
            bool: True if the required number of nodes are ready, False otherwise.
        """
        if self.kubernetes_n_nodes_ready():
            return True
        return False


class ArmoniKServicesHealthCheckSensor(BaseSensorOperator):
    """
    Sensor that checks the health status of ArmoniK services.

    Args:
        poke_interval (timedelta): Time interval between consecutive pokes.
        timeout (timedelta): Timeout duration for the sensor.
        armonik_conn_id (str, optional): Connection ID for ArmoniK. Default is 'armonik_default'.
    """

    def __init__(
        self,
        poke_interval: timedelta,
        timeout: timedelta,
        armonik_conn_id: str = "armonik_default",
        **kwargs,
    ) -> None:
        super().__init__(
            poke_interval=poke_interval, timeout=timeout, soft_fail=False, mode="poke", **kwargs
        )
        self.armonik_conn_id = armonik_conn_id

    def armonik_services_healthy(self) -> bool:
        """
        Checks if all ArmoniK services are healthy.

        Returns:
            bool: True if all services are healthy, False otherwise.
        """
        with GrpcHook(grpc_conn_id=self.armonik_conn_id).get_conn() as channel:
            client = ArmoniKHealthChecks(channel)

            for name, health in client.check_health().items():
                self.log.info(
                    f"Service {name} has status {health['status']}"
                    + (f"with message {health['message']}." if health["message"] else ".")
                )
                if health["status"] != ServiceHealthCheckStatus.HEALTHY:
                    return False
            return True

    def poke(self, context: Context) -> bool:
        """
        Performs a poke to check the health status of ArmoniK services.

        Args:
            context (Context): Airflow task context.

        Returns:
            bool: True if all services are healthy, False otherwise.
        """
        try:
            if self.armonik_services_healthy():
                return True
            return False
        except RpcError:
            return False
