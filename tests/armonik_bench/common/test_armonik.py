import uuid

import grpc
import pytest

from armonik.client import ArmoniKHealthChecks

from armonik_bench.common.armonik import armonik_services_healthy, update_workload_config


@pytest.mark.parametrize(
    ("healths", "expected_output"),
    [
        (
            {
                "database": {"message": "", "status": 0},
                "object": {"message": "", "status": 1},
                "queue": {"message": "", "status": 1},
            },
            False,
        ),
        (
            {
                "database": {"message": "", "status": 1},
                "object": {"message": "", "status": 1},
                "queue": {"message": "", "status": 1},
            },
            True,
        ),
        (
            {
                "database": {"message": "", "status": 2},
                "object": {"message": "", "status": 1},
                "queue": {"message": "", "status": 1},
            },
            False,
        ),
        (
            {
                "database": {"message": "", "status": 3},
                "object": {"message": "", "status": 1},
                "queue": {"message": "", "status": 1},
            },
            False,
        ),
    ],
)
def test_armonik_services_healthy(
    mocker, healths: dict[str, dict[str, str | int]], expected_output: bool
) -> None:
    with grpc.insecure_channel("host") as channel:
        mocker.patch.object(ArmoniKHealthChecks, "check_health", return_value=healths)
        assert armonik_services_healthy(channel) == expected_output


@pytest.mark.parametrize(("workload_config", "host", "port", "expected_output"), [
    ({"HtcMock__TotalCalculationTime": "00:00:00.0"}, "172.17.119.85", 5001, {"GrpcClient__Endpoint": "172.17.119.85:5001", "HtcMock__Options_UUID": ""}),
    ({"HtcMock__TotalCalculationTime": "00:00:00.0"}, "mycluster.com", None, {"GrpcClient__Endpoint": "mycluster.com", "HtcMock__Options_UUID": ""}),
    ({"BenchOptions__TotalCalculationTime": "00:00:00.0"}, "172.17.119.85", 5001, {"GrpcClient__Endpoint": "172.17.119.85:5001", "BenchOptions__Options_UUID": ""}),
])
def test_update_workload_config(mocker, workload_config: dict[str, str], host: str, port: int, expected_output: dict[str, str]) -> None:
    mocker.patch.object(uuid, "uuid4", return_value="")
    assert update_workload_config(workload_config, host, port) == (expected_output | workload_config)
