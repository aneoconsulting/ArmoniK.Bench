import datetime

import pytest

from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    return DagBag(
        include_examples=False,
    )


@pytest.fixture
def id_pattern() -> str:
    return r"^(?:[a-z]{3,}(?:[._][a-z]{2,})*)$"


def assert_dag_dict_equal(source: dict[str, list[str]], dag: DAG) -> None:
    assert dag.task_dict.keys() == source.keys()
    for task_id, downstream_list in source.items():
        assert dag.has_task(task_id)
        task = dag.get_task(task_id)
        assert task.downstream_task_ids == set(downstream_list)


def create_dagrun(
    dag: DAG, conf: dict[str, any], start_date: datetime.datetime, end_date: datetime.datetime
) -> DagRun:
    return dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=start_date,
        start_date=start_date,
        data_interval=(start_date, end_date),
        run_type=DagRunType.MANUAL,
        conf=conf,
    )


def assert_task_run_success(ti: TaskInstance) -> None:
    ti.run(ignore_task_deps=True, ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS


def assert_task_run_success_and_xcoms(
    mocker,
    ti: TaskInstance,
    *,
    pulls: int = 0,
    pushes: int = 0,
    pulls_kwargs: list[dict[str, any]] | None = None,
    pushes_kwargs: list[dict[str, any]] | None = None,
) -> None:
    def is_subdict(sub_dict: dict, super_dict: dict) -> bool:
        try:
            for key, value in sub_dict.items():
                if super_dict[key] != value:
                    return False
            return True
        except KeyError:
            return False

    spy_xcom_pull = mocker.spy(ti, "xcom_pull")
    spy_xcom_push = mocker.spy(ti, "xcom_push")

    assert_task_run_success(ti=ti)

    assert spy_xcom_pull.call_count >= pulls
    if pulls_kwargs:
        for pull_kwargs in pulls_kwargs:
            for call in spy_xcom_pull.call_args_list:
                assert is_subdict(pull_kwargs, call.kwargs)

    assert spy_xcom_push.call_count >= pushes
    if pushes_kwargs:
        for push_kwargs in pushes_kwargs:
            for call in spy_xcom_push.call_args_list:
                assert is_subdict(push_kwargs, call.kwargs)
