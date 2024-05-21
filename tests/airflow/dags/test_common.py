import re

from airflow.models import DagBag


def test_dag_exists(dag_bag: DagBag) -> None:
    assert sorted(dag_bag.dag_ids) == sorted(["run_campaign", "run_experiment"])


def test_no_dag_import_errors(dag_bag: DagBag) -> None:
    assert dag_bag.import_errors == {}


def test_dag_task_ids_format(dag_bag: DagBag, id_pattern: str) -> None:
    for dag_id in dag_bag.dag_ids:
        assert re.fullmatch(id_pattern, dag_id)
        for task_id in dag_bag.get_dag(dag_id=dag_id).task_dict.keys():
            assert re.fullmatch(id_pattern, task_id)
