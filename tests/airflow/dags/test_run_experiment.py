from datetime import datetime, timedelta, timezone

import pytest

from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag


DAG_ID = "run_experiment"


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
    assert [*dag.params.keys()] == ["experiment_id", "destroy"]
    assert dag.dagrun_timeout


# def test_dag_structure(dag: DAG) -> None:
#     assert_dag_dict_equal(
#         {
#             "deploy_armonik": ["warm_up.services_ready", "warm_up.nodes_ready"],
#             "warm_up.services_ready": ["prepare_workload_execution"],
#             "warm_up.nodes_ready": ["prepare_workload_execution"],
#             "prepare_workload_execution": ["run_client"],
#             "run_client": ["commit"],
#             "commit": ["skip_destroy"],
#             "skip_destroy": ["destroy_armonik"],
#             "destroy_armonik": [],
#         },
#         dag,
#     )


# def test_dag_conf_parsing(dag: DAG, dag_conf: dict[str, str]) -> None:
#     params = copy.deepcopy(dag.params)
#     params.update(dag_conf)
#     params = params.validate()
#     for param in dag_conf.items():
#         assert param in params.items()


# def test_task_warm_up_services_ready(mocker, dag: DAG, dagrun: DagRun, dag_namespace: str) -> None:
#     task_id = "warm_up.services_ready"
#     ti: TaskInstance = dagrun.get_task_instance(task_id=task_id)
#     ti.task = dag.get_task(task_id=task_id)

#     assert ti.task.task_type == "DecoratedSensorOperator"
#     assert ti.task.trigger_rule == "all_success"
#     assert ti.task.mode == "poke"
#     assert ti.task.poke_interval > 0
#     assert ti.task.timeout > ti.task.poke_interval

#     import grpc

#     with grpc.insecure_channel("host") as channel:
#         mock_armonik_services_healthy = MockSensorFunc(
#             returns=[False, grpc.RpcError(), False, True]
#         )

#         mocker.patch(
#             f"{dag_namespace}.Connection.get_connection_from_secrets", return_value=Connection()
#         )
#         mocker.patch(f"{dag_namespace}.GrpcHook.get_conn", return_value=channel)
#         mocker.patch(f"{dag_namespace}.armonik_services_healthy", new=mock_armonik_services_healthy)
#         assert_task_run_success(ti)
#         assert mock_armonik_services_healthy.call_count == 4
#         assert mock_armonik_services_healthy.raises == 1


# def test_task_warm_up_nodes_ready(mocker, dag: DAG, dagrun: DagRun, dag_namespace: str) -> None:
#     task_id = "warm_up.nodes_ready"
#     ti: TaskInstance = dagrun.get_task_instance(task_id=task_id)
#     ti.task = dag.get_task(task_id=task_id)

#     assert ti.task.task_type == "DecoratedSensorOperator"
#     assert ti.task.trigger_rule == "all_success"
#     assert ti.task.mode == "poke"
#     assert ti.task.poke_interval > 0
#     assert ti.task.timeout > ti.task.poke_interval

#     mock_kubernetes_n_nodes_ready = MockSensorFunc(returns=[False, False, True])

#     mocker.patch("kubernetes.config.load_incluster_config")
#     mocker.patch(f"{dag_namespace}.KubernetesHook.core_v1_client", return_value=CoreV1Api())
#     mocker.patch(f"{dag_namespace}.kubernetes_n_nodes_ready", new=mock_kubernetes_n_nodes_ready)
#     assert_task_run_success_and_xcoms(mocker, ti, pushes=0)
#     assert mock_kubernetes_n_nodes_ready.call_count == 3


# def test_prepare_workload_execution(mocker, dag: DAG, dagrun: DagRun) -> None:
#     task_id = "prepare_workload_execution"
#     ti: TaskInstance = dagrun.get_task_instance(task_id=task_id)
#     ti.task = dag.get_task(task_id=task_id)

#     assert ti.task.task_type == "_PythonDecoratedOperator"
#     assert ti.task.trigger_rule == "all_success"

#     mocker.patch.object(
#         Connection,
#         "get_connection_from_secrets",
#         return_value=Connection(conn_id="conn_id", host="host", port=10),
#     )
#     mocker.patch.object(uuid, "uuid4", return_value="")
#     assert_task_run_success_and_xcoms(
#         mocker,
#         ti,
#         pulls=0,
#         pushes=1,
#         pushes_kwargs=[
#             {
#                 "key": "workload_config",
#                 "value": {"GrpcClient__Endpoint": "http://host:10", "GrpcClient__Options_UUID": ""},
#             }
#         ],
#     )


# def test_task_run_client(mocker, dag: DAG, dagrun: DagRun, dag_conf: dict[str, str]) -> None:
#     task_id = "run_client"
#     ti: TaskInstance = dagrun.get_task_instance(task_id=task_id)
#     ti.task = dag.get_task(task_id=task_id)

#     assert ti.task.task_type == "ExtraTemplatedKubernetesJobOperator"
#     assert ti.task.trigger_rule == "all_success"

#     pull_value = {
#         "GrpcClient__Endpoint": "http://host:10",
#         "GrpcClient__Options_UUID": "",
#     } | dag_conf["workload_config"]

#     mocker.patch("kubernetes.config.load_incluster_config")
#     mocker.patch.object(KubernetesHook, "create_job", return_value=None)
#     mocker.patch.object(TaskInstance, "xcom_pull", return_value=pull_value)
#     assert_task_run_success_and_xcoms(
#         mocker,
#         ti,
#         pulls=1,
#         pulls_kwargs=[{"task_ids": "prepare_workload_execution", "key": "workload_config"}],
#     )

#     assert ti.task.job_request_obj.metadata.namespace == "armonik"
#     assert "job-run-client" in ti.task.job_request_obj.metadata.name
#     assert "run-client" in ti.task.job_request_obj.spec.template.metadata.name
#     assert ti.task.job_request_obj.spec.template.spec.containers[0].image == dag_conf["workload"]
#     assert ti.task.job_request_obj.spec.template.spec.containers[0].env == pull_value


# def test_task_commit():
#     pass


# @pytest.mark.parametrize("dag_conf", [True, False], indirect=True)
# def test_task_skip_destroy(mocker, dag: DAG, dagrun: DagRun, dag_conf: dict[str, any]) -> None:
#     task_id = "skip_destroy"
#     ti: TaskInstance = dagrun.get_task_instance(task_id=task_id)
#     ti.task = dag.get_task(task_id=task_id)

#     assert ti.task.task_type == "_ShortCircuitDecoratedOperator"
#     assert ti.task.trigger_rule == "all_done"

#     assert_task_run_success_and_xcoms(
#         mocker, ti, pushes=1, pushes_kwargs=[{"key": "return_value", "value": dag_conf["destroy"]}]
#     )
