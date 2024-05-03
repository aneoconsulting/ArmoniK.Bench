"""
# Run Experiment DAG

## Objective
This Airflow DAG is designed to perform a single benchmark experiment for the computing platform
ArmoniK. The experiment involves deploying or updating the existing infrastructure, running a
workload on the deployed cluster, retrieving data from the workload run for future post-processing,
and optionally destroying the infrastructure based on specified parameters.

## Trigger
This DAG is not triggered automatically and does not catch up on missed runs. It is intended to be
triggered manually by another DAG or a user.

## Parameters
The DAG accepts the following parameters:
- `destroy`: A boolean parameter specifying whether infrastructure destruction is needed after the
  experiment. Default is False.

## Prerequisites
To run this DAG, the DAG must have the required credentials and associated permissions to deploy
ArmoniK on the specified environment. These credentials are the ones required by Terraform.

## Environment Variables
This DAG accepts the following optional environment variables:
- `DAG__RUN_EXPERIMENT__MAX_ACTIVE_TASKS`: Specifies the maximum number of active tasks allowed.
  Default is 10.
- `DAG__RUN_EXPERIMENT__DAGRUN_TIMEOUT`: Specifies the timeout for the DAG run. Default is 5 hours.

## Interactions
- **ArmoniK Cluster**: The DAG interacts with the deployed ArmoniK cluster to monitor, run
  workloads, and extract run data.
- **Google Cloud Storage**: Interaction occurs to store execution data and outputs.

## Failure Handling
In case of failure, the DAG will notify via email (see Airflow configuration).

## Outputs
The outputs of the DAG are stored in the 'data' folder of Google Cloud Storage bucket associated
with the Cloud Composer environment.
"""

import os

from datetime import datetime, timedelta, timezone

from airflow.decorators import dag


@dag(
    dag_id="run_experiment",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    # UI parameters
    description="Carry out a benchmark experiment on a given ArmoniK application and deployment",
    doc_md=__doc__,
    # Scaling parameters
    max_active_tasks=int(os.environ.get("DAG__RUN_EXPERIMENT__MAX_ACTIVE_TASKS", 10)),
    max_active_runs=1,
    # Other paramters
    end_date=None,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
    params={},
    dagrun_timeout=timedelta(
        minutes=int(os.environ.get("DAG__RUN_EXPERIMENT__DAGRUN_TIMEMOUT", 300))
    ),
)
def run_experiment():
    pass


run_experiment()
