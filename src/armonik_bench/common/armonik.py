import json
import uuid

from armonik.client import ArmoniKHealthChecks
from armonik.common import ServiceHealthCheckStatus
from grpc import Channel


def armonik_services_healthy(channel: Channel) -> bool:
    """Returns True if ArmoniK services (state database, queue and object storage) are healthy, False if not.

    Parameters
    ----------
    channel : grpc.Channel
        gRPC channel to the control plane of a running ArmoniK cluster

    Returns
    -------
    bool
        True if all services are healthy, False otherwise.

    Raises
    ------
    RpcError
        If an error occurs in the communication with the control plane
    """
    client = ArmoniKHealthChecks(channel)

    for name, health in client.check_health().items():
        if health["status"] != ServiceHealthCheckStatus.HEALTHY:
            return False
        return True


def update_workload_config(
    workload_config: dict[str, str], host: str, port: int | None = None
) -> dict[str, str]:
    """Updates the user-provided workload configuration by adding the url of the cluster control plane on which the workload will run,
    as well as a taint to identify the workload's tasks after execution.
    The taint is simply a uuid added to the task options of the workload.

    Parameters
    ----------
    workload_config : dict[str, str]
        User-provided configuration for the workload.
    host : str
        Host of the control plane of the ArmoniK cluster on which the workload will run.
    port : int | None
        Port of the control plane url of the ArmoniK cluster on which the workload will run. Optional, default is None.

    Returns
    -------
    dict[str, str]
        The updated workload configuration containing the url of the cluster control plane and the taint.
    """
    workload_config["GrpcClient__Endpoint"] = (
        f"http://{host}" if not port else f"http://{host}:{port}"
    )
    id = str(uuid.uuid4())
    workload_config[f"{[*workload_config.keys()][0].split('_')[0]}__Options_UUID"] = id
    return {
        key: json.dumps(value) if not isinstance(value, str) else value
        for key, value in workload_config.items()
    }, id
