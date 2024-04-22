from typing import Sequence

from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import convert_env_vars
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.utils.context import Context
from kubernetes.client import models as k8s


class ExtraTemplatedKubernetesJobOperator(KubernetesJobOperator):
    template_fields: Sequence[str] = tuple(
        {"env_vars"} | set(KubernetesJobOperator.template_fields)
    )
    template_fields_renderers = {"env_vars": "json"}

    def __init__(self, *, env_vars, **kwargs):
        super().__init__(**kwargs)
        self.env_vars = env_vars

    def execute(self, context: Context):
        self.env_vars = convert_env_vars(self.env_vars)
        if context["params"]["environment"] != "localhost":
            self.node_selector={"service": "others"},
            self.tolerations=[k8s.V1Toleration(effect="NoSchedule", key="service", value="others")],
        return super().execute(context)
