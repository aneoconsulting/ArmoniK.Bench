import json

import docker

from kubernetes import client, config

from armonik_bench.operators.armonik import (
    ArmoniKDeployClusterOperator,
    ArmoniKDestroyClusterOperator,
)


RELEASE = "v2.18.2"
ENVIRONMENT = "localhost"
CORE_VERSION = "0.20.5"
CONFIG = {
    "logging_level": "Information",
    "redis": {},
    "activemq": {},
    "metrics_exporter": {
        "extra_conf": {
            "MongoDB__AllowInsecureTls": True,
            "Serilog__MinimumLevel": "Information",
            "MongoDB__TableStorage__PollingDelayMin": "00:00:01",
            "MongoDB__TableStorage__PollingDelayMax": "00:00:10",
            "MongoDB__DataRetention": "1.00:00:00",
        }
    },
    "control_plane": {
        "limits": {"cpu": "1000m", "memory": "2048Mi"},
        "requests": {"cpu": "50m", "memory": "50Mi"},
        "default_partition": "default",
    },
    "admin_gui": {
        "limits": {"cpu": "1000m", "memory": "1024Mi"},
        "requests": {"cpu": "100m", "memory": "128Mi"},
        "node_selector": {"service": "monitoring"},
    },
    "compute_plane": {
        "htcmock": {
            "replicas": 1,
            "polling_agent": {
                "limits": {"cpu": "2000m", "memory": "2048Mi"},
                "requests": {"cpu": "50m", "memory": "50Mi"},
            },
            "worker": [
                {
                    "image": "dockerhubaneo/armonik_core_htcmock_test_worker",
                    "limits": {"cpu": "1000m", "memory": "1024Mi"},
                    "requests": {"cpu": "50m", "memory": "50Mi"},
                }
            ],
        }
    },
    "ingress": {"tls": False, "mtls": False, "generate_client_cert": False},
    "extra_conf": {
        "core": {
            "Amqp__AllowHostMismatch": True,
            "Amqp__MaxPriority": "10",
            "Amqp__MaxRetries": "5",
            "Amqp__QueueStorage__LockRefreshPeriodicity": "00:00:45",
            "Amqp__QueueStorage__PollPeriodicity": "00:00:10",
            "Amqp__QueueStorage__LockRefreshExtension": "00:02:00",
            "MongoDB__TableStorage__PollingDelayMin": "00:00:01",
            "MongoDB__TableStorage__PollingDelayMax": "00:00:10",
            "MongoDB__AllowInsecureTls": True,
            "MongoDB__TableStorage__PollingDelay": "00:00:01",
            "MongoDB__DataRetention": "1.00:00:00",
            "Redis__Timeout": 30000,
            "Redis__SslHost": "127.0.0.1",
            "Redis__TtlTimeSpan": "1.00:00:00",
        },
        "control": {"Submitter__MaxErrorAllowed": 50},
        "worker": {"target_zip_path": "/tmp"},
    },
    "jobs_in_database_extra_conf": {"MongoDB__DataRetention": "1.00:00:00"},
    "environment_description": {
        "name": "local-dev",
        "version": "0.0.0",
        "description": "Local development environment",
        "color": "blue",
    },
    "chaos_mesh": {},
}


def test_armonik_deploy_cluster(mock_context):
    op = ArmoniKDeployClusterOperator(
        task_id="test-deploy-cluster", release=RELEASE, environment=ENVIRONMENT, config=CONFIG
    )
    op.execute(mock_context)
    client = docker.from_env()
    container = client.containers.run(
        f"dockerhubaneo/armonik_core_htcmock_test_client:{CORE_VERSION}",
        environment={
            "GrpcClient__Endpoint": "http://172.17.119.85:5001",
            "HtcMock__NTasks": "100",
            "HtcMock__TotalCalculationTime": "00:00:00.100",
            "HtcMock__DataSize": "1",
            "HtcMock__MemorySize": "1",
            "HtcMock__SubTasksLevels": "10",
            "HtcMock__EnableUseLowMem": True,
            "HtcMock__EnableSmallOutput": True,
            "HtcMock__EnableFastCompute": True,
            "HtcMock__Partition": "htcmock",
        },
        # Run the container in the background
        detach=True,
    )

    # Wait for the container to finish
    container.wait()

    # Retrieve stdout and stderr logs
    stdout_str = container.logs(stdout=True, stderr=False).decode("utf-8")
    stderr_str = container.logs(stdout=False, stderr=True).decode("utf-8")

    # Remove the container when it's done
    container.remove()

    assert not stderr_str
    assert stdout_str
    outputs = json.loads(stdout_str.split("\n")[-2])
    assert outputs["throughput"] > 0.0
    assert outputs["nTasks"] == 150
    assert outputs["session"]


def test_armonik_destroy_cluster(mock_context):
    op = ArmoniKDestroyClusterOperator(
        task_id="test-destroy-cluster", release=RELEASE, environment=ENVIRONMENT, config=CONFIG
    )
    op.execute(mock_context)

    config.load_kube_config()
    v1 = client.CoreV1Api()
    namespaces = v1.list_namespace().items
    assert "armonik" not in [namespace.metadata.name for namespace in namespaces]
