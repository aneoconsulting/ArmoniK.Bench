from typing import Sequence

from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.utils.context import Context


class ExtraTemplatedKubernetesJobOperator(KubernetesJobOperator):
    template_fields: Sequence[str] = tuple(
        {"env_vars"} | set(KubernetesJobOperator.template_fields)
    )

    def __init__(self, *, env_vars, **kwargs):
        super().__init__(**kwargs)
        self.env_vars = env_vars

    def execute(self, context: Context):
        self.test_vars = self.env_vars
        return super().execute(context)
