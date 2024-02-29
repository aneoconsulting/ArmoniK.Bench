import datetime

import pytest
from airflow import DAG

@pytest.fixture
def test_dag():
    return DAG(
        “test_dag”,
        default_args={“owner”: “airflow”, “start_date”: datetime.datetime(2024, 1, 1)},
        schedule_interval=None,
    )
