from typing import Sequence

from airflow.exceptions import AirflowException, AirflowFailException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.context import Context


class RunExperiments(TriggerDagRunOperator):
    """
    Custom operator to run experiments.

    This operator extends the TriggerDagRunOperator to trigger 'run_experiment' DAG in a loop.

    Args:
        experiment_ids (list[str]): List of experiment IDs to run.
        allowed_failures (int | None): Number of failed experience runs allowed before the DAG is
            marked as having failed. Defaults to None.
        poke_interval (int): Interval (in seconds) between each poll while waiting for the
            experiments to complete. Defaults to 60s.
    """

    template_fields: Sequence[str] = ("experiment_ids",)
    __run_experiment_dag_id = "run_experiment"

    def __init__(
        self,
        experiment_ids: list[str],
        allowed_failures: int | None = None,
        poke_interval: int = 60,
        **kwargs,
    ) -> None:
        super().__init__(
            trigger_dag_id=self.__run_experiment_dag_id,
            reset_dag_run=True,
            wait_for_completion=True,
            poke_interval=poke_interval,
            allowed_states=["success"],
            **kwargs,
        )
        self.experiment_ids = experiment_ids
        self.allowed_failures = allowed_failures

    def execute(self, context: Context):
        """
        Execute each of the experiments by successively triggering an execution of the
        'run_experiment' DAG.

        Args:
            context (Context): The context passed by Airflow during task execution.
        """
        num_failures = 0
        for index, experiment_id in enumerate(self.experiment_ids):
            self.log.info(f"Running experiment {experiment_id}")
            try:
                self.conf = {
                    "experiment_id": experiment_id,
                    "destroy": True if index == len(self.experiment_ids) - 1 else False,
                }
                super().execute(context)
            except AirflowException as error:
                self.log.warning(f"An experiment run failed with error {error}.")
                num_failures += 1
                if self.allowed_failures is not None and num_failures >= self.allowed_failures:
                    raise AirflowFailException(
                        f"Stopped after {num_failures} unsuccessful experiment runs."
                    )
