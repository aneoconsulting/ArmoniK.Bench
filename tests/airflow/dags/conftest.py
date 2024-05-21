import re

from datetime import datetime
from pathlib import Path
from typing import Generator, Any

import pytest

from airflow.models import DAG, DagBag, DagRun, TaskInstance
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from airflow.utils.session import NEW_SESSION, provide_session
from sqlalchemy.orm.session import Session


@pytest.fixture(scope="session")
def dag_bag() -> DagBag:
    """
    Fixture to provide a DagBag instance configured to read DAGs from src/airflow/dags directory.

    Returns:
        DagBag: An instance of DagBag containing the project DAGs.
    """
    return DagBag(
        dag_folder=str(Path(__file__).parent.parent.parent.parent / "dags"),
        include_examples=False,
        read_dags_from_db=False,
    )


@pytest.fixture(scope="session")
def id_pattern() -> re.Pattern:
    """
    Fixture to provide a compiled regular expression pattern for DAGs and tasks ID validation.
    DAG and task IDs must:
    - contain only lower-case alphabetic characters and underscores (_);
    - begin with at least three alphabetical characters;
    - end with at least two alphabetical characters.

    Returns:
        re.Pattern: A compiled regular expression object.
    """
    return re.compile(r"^(?:[a-z]{3,}(?:[._][a-z]{2,})*)$")


@pytest.fixture(scope="function")
def dag_run(request: Any, dag_bag: DagBag) -> Generator[DagRun, None, None]:
    """
    Fixture to create and clean up a DagRun for task testing.

    Args:
        request (Any): The request object providing access to test parameters.
        dag_bag (DagBag): The DagBag instance containing the DAGs.

    Yields:
        Generator[DagRun, None, None]: A generator yielding the created DagRun instance.
    """

    def create_dag_run(dag: DAG, conf: dict | None = None) -> DagRun:
        """
        Create a new DagRun for the specified DAG.

        Args:
            dag (DAG): The DAG for which to create the DagRun.
            conf (dict | None): The configuration dictionary for the DagRun.

        Returns:
            DagRun: The created DagRun instance.
        """
        start_date = datetime.now()
        return dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=start_date,
            start_date=start_date,
            data_interval=None,
            run_type=DagRunType.MANUAL,
            conf=conf,
        )

    @provide_session
    def delete_dag_run(dag_run: DagRun, session: Session = NEW_SESSION) -> None:
        """
        Delete the specified DagRun from the database.

        Args:
            dag_run (DagRun): The DagRun to delete.
            session (Session): The database session to use for the deletion.
        """
        session.delete(dag_run)
        session.commit()

    try:
        if hasattr(request, "param"):
            dag_id = request.param.dag_id
            conf = request.param.conf
        elif hasattr(request.node, "callspec"):
            dag_id = request.node.callspec.params["dag_id"]
            conf = request.node.callspec.params["conf"]
        else:
            raise RuntimeError("Test function didn't provide the required parameters for the fixture.")
    except KeyError as error:
        raise ValueError(f"Missing fixture required argument: {error}.")

    dag = dag_bag.get_dag(dag_id=dag_id)
    dag_run_instance = create_dag_run(dag, conf)
    yield dag_run_instance
    delete_dag_run(dag_run_instance)


def assert_dag_dict_equal(source: dict[str, list[str]], dag: DAG) -> None:
    """
    Assert that the task dictionary of a DAG matches the expected source dictionary.

    Args:
        source (dict[str, list[str]]): The source dictionary with task IDs and their downstream tasks.
        dag (DAG): The DAG to validate against the source dictionary.
    """
    assert dag.task_dict.keys() == source.keys()
    for task_id, downstream_list in source.items():
        assert dag.has_task(task_id)
        task = dag.get_task(task_id)
        assert task.downstream_task_ids == set(downstream_list)


def assert_xcom_pull(ti: TaskInstance, task_ids: list[str], key: str, value: Any) -> None:
    pass


def assert_xcom_push(mocker: Any, ti: TaskInstance, key: str, value: Any):
    pass


# @pytest.fixture(scope="module")
# def dag_namespace(dag_bag: DagBag, ) -> str:
#     try:
#         from airflow.utils.file import get_unique_dag_module_name

#         mod_name = get_unique_dag_module_name(dag.fileloc)
#     except ImportError:
#         import pathlib
#         import hashlib

#         filepath = dag.fileloc
#         path_hash = hashlib.sha1(filepath.encode("utf-8")).hexdigest()
#         org_mod_name = pathlib.Path(filepath).stem
#         mod_name = f"unusual_prefix_{path_hash}_{org_mod_name}"

#     return mod_name
