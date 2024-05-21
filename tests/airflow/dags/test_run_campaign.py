from datetime import datetime, timedelta, timezone

import pytest

from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag


DAG_ID = "run_campaign"


@pytest.fixture(scope="module")
def dag(dag_bag: DagBag) -> DAG:
    return dag_bag.get_dag(dag_id=DAG_ID)


def test_dag_options(dag: DAG) -> None:
    assert dag.start_date < datetime.now(tz=timezone.utc)
    assert dag.schedule_interval is None
    assert dag.dataset_triggers == []
    assert not dag.catchup
    assert dag.description
    assert dag.doc_md
    assert dag.render_template_as_native_obj
    assert dag.max_active_tasks > 0
    assert dag.max_active_runs == 1
    assert dag.end_date is None
    assert dag.default_args["owner"]
    assert dag.default_args["retries"] >= 0
    assert dag.default_args["retry_delay"]
    assert [*dag.params.keys()] == ["campaign_id"]
    assert dag.dagrun_timeout
