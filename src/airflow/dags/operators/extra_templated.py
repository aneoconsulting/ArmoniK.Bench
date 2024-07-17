from typing import Sequence

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.context import Context
from kubernetes.client import models as k8s


class CustomKubernetesPodOperator(KubernetesPodOperator):
    template_fields: Sequence[str] = (*KubernetesPodOperator.template_fields, "working_dir")

    def __init__(self, working_dir: str = None, **kwargs) -> None:
        if "full_pod_spec" in kwargs.keys():
            raise ValueError(
                "This KubernetesPodOperator wrapper does not support the use of the 'full_pod_spec' attribute."
            )
        super().__init__(**kwargs)
        self.working_dir = working_dir

    def execute(self, context: Context):
        if self.working_dir:
            self.full_pod_spec = k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            working_dir=self.working_dir,
                            name="base",
                        )
                    ]
                )
            )
        return super().execute(context)
