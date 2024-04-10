import re

from airflow.models import DagBag


def test_dag_exists(dagbag: DagBag) -> None:
    assert sorted(dagbag.dag_ids) == sorted(["run_campaign", "run_experiment"])


def test_no_dag_import_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}


def test_dag_task_ids_format(dagbag: DagBag, id_pattern: str) -> None:
    for dag_id in dagbag.dag_ids:
        assert re.fullmatch(id_pattern, dag_id)
        for task_id in dagbag.get_dag(dag_id=dag_id).task_dict.keys():
            assert re.fullmatch(id_pattern, task_id)
