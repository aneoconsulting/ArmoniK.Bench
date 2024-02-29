import copy

from airflow.models import DagBag


RUN_EXPERIMENT_CONF = {
    "exp_name": "test",
    "release": "v2.18.2",
    "environment": "localhost",
    "infra_config": {},
    "infra_region": "europ-west1",
    "workload": "image",
    "workload_config": {},
    "client_instance_zone": "europ-west1-d",
}

RUNNER_CONF = {
    "campaign": "test",
}


def test_run_experiment_params():
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag("armonik-run-experiment")
    copied_params = copy.deepcopy(dag.params)
    copied_params.update(RUN_EXPERIMENT_CONF)
    copied_params.validate()


def test_runner_params():
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag("armonik-benchmark-runner")
    copied_params = copy.deepcopy(dag.params)
    copied_params.update(RUNNER_CONF)
    copied_params.validate()
