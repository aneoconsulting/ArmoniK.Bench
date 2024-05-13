import uuid

from typing import Sequence

from airflow.models.connection import Connection
from airflow.models.taskinstance import TaskInstance
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.utils.context import Context
from kubernetes.client import models as k8s


class RunArmoniKClientOperator(KubernetesJobOperator):
    template_fields: Sequence[str] = tuple({"config", *KubernetesJobOperator.template_fields})

    def __init__(
        self,
        image: str,
        config: dict[str, str] | None = None,
        armonik_conn_id: str = "armonik_default",
        armonik_kubernetes_conn_id: str = "armonik_kubernetes_default",
        **kwargs,
    ) -> None:
        super().__init__(
            name="run-client",
            namespace="armonik",
            labels={"app": "armonik", "service": "run-client", "type": "others"},
            image=image,
            backoff_limit=1,
            completion_mode="NonIndexed",
            completions=1,
            parallelism=1,
            node_selector={"service": "others"},
            tolerations=[k8s.V1Toleration(effect="NoSchedule", key="service", value="others")],
            on_finish_action="delete_pod",
            wait_until_job_complete=True,
            kubernetes_conn_id=armonik_kubernetes_conn_id,
            **kwargs,
        )
        self.config = config
        self.armonik_conn_id = armonik_conn_id

    def get_endpoint(self) -> str:
        conn = Connection.get_connection_from_secrets(conn_id=self.armonik_conn_id)
        return f"http://{conn.host}:{conn.port}"

    def get_workload_env_var_prefix(self):
        return [*self.config.keys()][0].split("__")[0]

    def execute(self, context: Context):
        job_uuid = str(uuid.uuid4())
        self.env_vars = [k8s.V1EnvVar(name=key, value=value) for key, value in self.config.items()]
        self.env_vars.extend(
            [
                k8s.V1EnvVar(name="GrpcClient__Endpoint", value=self.get_endpoint()),
                k8s.V1EnvVar(
                    name=f"{self.get_workload_env_var_prefix()}__Options__UUID", value=job_uuid
                ),
            ]
        )

        ti: TaskInstance = context["ti"]
        ti.xcom_push(key="job_uuid", value=job_uuid)

        return super().execute(context)
