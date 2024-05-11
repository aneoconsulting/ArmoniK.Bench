from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

import polars as pl

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.grpc.hooks.grpc import GrpcHook
from airflow.utils.context import Context
from armonik.client import ArmoniKTasks, TaskFieldFilter
from armonik.common import Direction
from grpc import RpcError


class ArmoniKDumpData(BaseOperator):
    template_fields: Sequence[str] = tuple({"job_uuid"})

    def __init__(
        self, job_uuid: str, data_dir: Path, armonik_conn_id: str = "armonik_default", **kwargs
    ):
        super().__init__(**kwargs)
        self.job_uuid = job_uuid
        self.data_dir = data_dir
        self.armonik_conn_id = armonik_conn_id

    def pull_tasks(self) -> pl.DataFrame:
        try:
            with GrpcHook(grpc_conn_id=self.armonik_conn_id).get_conn() as channel:
                task_client = ArmoniKTasks(channel)

                list_options = {
                    "task_filter": (TaskFieldFilter.task_options_key("UUID") == self.job_uuid),
                    "with_errors": True,
                    "page_size": 1000,
                    "sort_field": TaskFieldFilter.TASK_ID,
                    "sort_direction": Direction.ASC,
                    "detailed": True,
                }

                page = 0
                total, tasks = task_client.list_tasks(**list_options, page=page)
                df = pl.DataFrame(tasks)
                while tasks:
                    page += 1
                    _, tasks = task_client.list_tasks(**list_options, page=page)
                    df = df.vstack(pl.DataFrame(tasks))
                return df
        except RpcError as error:
            raise AirflowException(error)

    def execute(self, context: Context) -> Any:
        ti = context["ti"]
        params = context["params"]
        run_id = f"{params['experiment_id']}_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')}"
        ti.xcom_push(key="run_id", value=run_id)

        df = self.pull_tasks()
        df.write_parquet(self.data_dir / f"dumps/{run_id}.parquet")
