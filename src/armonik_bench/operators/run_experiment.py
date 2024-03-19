from typing import Sequence

from airflow.exceptions import AirflowException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.context import Context


class RunExperiment(TriggerDagRunOperator):
    template_fields: Sequence[str] = ("campaign_data",)
    __run_experiment_dag_id = "armonik-run-experiment"

    def __init__(
        self,
        *,
        campaign_data: dict[str, any],
        stop_on_failure: bool = False,
        allowed_failures: int | None = None,
        retry_on_preemption: bool = True,
        poke_interval: int = 60,
        **kwargs,
    ) -> None:
        self.campaign_data = campaign_data
        self.stop_on_failure = stop_on_failure
        self.allowed_failures = allowed_failures
        self.retry_on_preemption = retry_on_preemption
        self.poke_interval = poke_interval
        super().__init__(
            trigger_dag_id=self.__run_experiment_dag_id,
            reset_dag_run=True,
            wait_for_completion=True,
            poke_interval=self.poke_interval,
            allowed_states=["success"],
            **kwargs,
        )

    def execute(self, context: Context):
        failures_counter = 0
        for experiment in self.campaign_data["experiments"]:
            self.log.info(f"Running experiment {experiment['name']}")
            try:
                self.conf = {
                    "exp_name": experiment["name"],
                    "release": self.campaign_data["release"],
                    "environment": self.campaign_data["environment"],
                    "infra_region": experiment["infrastructure"]["region"],
                    "infra_config": experiment["infrastructure"]["config"],
                    "workload": experiment["workload"]["image"],
                    "workload_config": experiment["workload"]["config"],
                    "client_instance_name": self.campaign_data["client"]["instance_name"],
                    "client_instance_zone": self.campaign_data["client"]["instance_zone"],
                    "armonik_conn_id": context["params"]["armonik_conn_id"],
                    "github_conn_id": context["params"]["github_conn_id"],
                    "bucket_prefix": context["params"]["bucket_prefix"],
                    "gcp_conn_id": context["params"]["gcp_conn_id"],
                }
                super().execute(context)
            except AirflowException:
                if self.stop_on_failure:
                    raise AirflowException(
                        "RunExperiment operator error: stopped after first unsuccessful experiment run."
                    )
                failures_counter += 1
                if self.allowed_failures and failures_counter >= self.allowed_failures:
                    raise AirflowException(
                        f"RunExperiment operator error: stopped after {failures_counter} unsuccessful experiment runs."
                    )
