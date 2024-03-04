import os
import json
from argparse import ArgumentParser
from hashlib import sha256
from itertools import product
from pathlib import Path


import hcl
from airflow.io.path import ObjectStoragePath
from jinja2 import Environment, FileSystemLoader


base = ObjectStoragePath(os.environ["AIRFLOW_OBJECT_STORAGE_PATH"])


def render_jinja_template(
    *,
    template_file: Path,
    **kwargs,
) -> None:
    environment = Environment(loader=FileSystemLoader(template_file.parent))
    template = environment.get_template(template_file.name)

    content = template.render(**kwargs)

    return content


def save(data: dict[str, str], *, type: str) -> Path:
    path = (base / f"{type}/{sha256(json.dumps(data).encode('utf-8')).hexdigest()}")
    with path.open("w") as file:
        file.write(json.dumps(data))
    return path


def workload_generator(version: str, n_task: int, sub_tasks_levels: int, total_calculationt_time: str | None = None, data_size: int | None = None, task_rpc_exception: bool = False) -> Path:
    workload = {
        "image": f"dockerhubaneo/armonik_core_htcmock_test_client:{version}",
        "config": {
            "HtcMock__NTasks": n_task,
            "HtcMock__TotalCalculationTime": total_calculationt_time if total_calculationt_time else "00:00:00.0",
            "HtcMock__DataSize": data_size if data_size else "0",
            "HtcMock__MemorySize": "0",
            "HtcMock__SubTasksLevels": sub_tasks_levels,
            "HtcMock__EnableFastCompute": False if total_calculationt_time else True,
            "HtcMock__EnableUseLowMem": True,
            "HtcMock__EnableSmallOutput": False if data_size else True,
            "HtcMock__TaskRpcException": "a" if task_rpc_exception else "",
            "HtcMock__TaskError": "",
            "HtcMock__Partition": "htcmock",
        }
    }
    return save(workload, type="workloads")

def infrastructure_generator(environment: str, n_nodes: int, infra_region: str) -> Path:
    infra_config = hcl.loads(
        render_jinja_template(
            template_file=Path(__file__).parent.parent / f"resources/armonik/{environment}.tfvars.template",
            n_nodes=n_nodes,
        )
    )
    return save({"region": infra_region, "config": infra_config}, type="infrastructures")


def campaign_generator(
        release: str,
        environment: str,
        instance_name: str,
        instance_zone: str,
        instance_region: str,
        infra_region: str,
        machine_type: str,
        nodes: list[int],
        workloads: list[dict[str, str]],
    ) -> None:
    campaign = {
        "release": release,
        "environment": environment,
        "client": {
            "instance_name": instance_name,
            "instance_zone": instance_zone,
            "instance_region": instance_region,
            "machine_type": machine_type,
        },
        "experiments": [
            {
                "workload": workload_generator(release, workload["n_tasks"], workload["sub_tasks_levels"], workload["total_calculation_time"], workload["data_size"], workload["task_rpc_exception"]).name,
                "infrastructure": infrastructure_generator(environment, n_nodes, infra_region).name,
            } for n_nodes, workload in product(nodes, workloads)
        ]
    }

    return save(campaign, type="campaigns")


if __name__ == "__main__":
    parser = ArgumentParser("Simple script to generate a new benchmark campaign running Htc Mock on an inelastic infrastructure on GCP.")
    parser.add_argument("-c", "--config", type=str, help="Configuration file")
    args = parser.parse_args()

    with Path(args.config).open() as file:
        print(f"Campaign hash {campaign_generator(**json.loads(file.read())).name}")
