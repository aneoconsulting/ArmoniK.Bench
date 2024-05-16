"""
# Run Campaign DAG

## Objective
This Airflow DAG is designed to supervise the execution of a benchmark campaign for the ArmoniK
computing platform. This involves managing the execution of the various benchmark experiments
making up the campaign. This DAG performs a few additional operations, such as checking the
validity of the experiments or supervising post-processing.

## Trigger
This DAG is not triggered automatically and does not catch up on missed runs. It is intended to be
triggered manually by a user.

## Parameters
The DAG accepts the following parameters:
- `campaign_id`: A string parameter specifying the campaign to be run by the workflow.

## Prerequisites
This DAG has no direct requirements, but it triggers the 'run_experiment' DAG, which has several.

## Environment Variables
This DAG accepts the following optional environment variables:
- `DAG__RUN_CAMPAIGN__MAX_ACTIVE_TASKS`: Specifies the maximum number of active tasks allowed.
  Default is 10.
- `DAG__RUN_CAMPAIGN__DAGRUN_TIMEOUT`: Specifies the timeout for the DAG run. Default is 5 hours.

## Interactions
- **Google Cloud Storage**: Interaction occurs to load campaign data and store post-processing outputs.

## Failure Handling
In case of failure, the DAG will notify via email (see Airflow configuration).

## Outputs
The outputs of the DAG are stored in the 'data' folder of Google Cloud Storage bucket associated
with the Cloud Composer environment.
"""

import json
import os

from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param


data_dir = Path("/home/airflow/gcs/data")


@dag(
    dag_id="run_campaign",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    # UI parameters
    description="Manages the execution of a benchmark campaign",
    doc_md=__doc__,
    # Jinja templating parameters
    render_template_as_native_obj=True,
    # Scaling parameters
    max_active_tasks=int(os.environ.get("DAG__RUN_CAMPAIGN__MAX_ACTIVE_TASKS", 10)),
    max_active_runs=1,
    # Other paramters
    end_date=None,
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
    },
    params={
        "campaign_id": Param(
            default="", description="ID of the campaign to be run by the workflow", type="string"
        ),
    },
    dagrun_timeout=timedelta(
        minutes=int(os.environ.get("DAG__RUN_CAMPAIGN__DAGRUN_TIMEMOUT", 300))
    ),
)
def run_campaign():
    @task
    def load_campaign(params: dict[str, str] | None = None) -> list[str]:
        campaign_id = params["campaign_id"]

        if not campaign_id:
            raise AirflowFailException("Campaign ID is empty.")

        with (data_dir / f"campaigns/{campaign_id}").open() as f:
            return json.loads(f.read())["experiments"]

    load_campaign()


run_campaign()
