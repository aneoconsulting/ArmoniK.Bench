from time import sleep
from typing import Sequence

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import (
    convert_env_vars,
)
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.utils.context import Context
from kubernetes.client import models as k8s


class ExtraTemplatedKubernetesJobOperator(KubernetesJobOperator):
    template_fields: Sequence[str] = tuple(
        {"env_vars"} | set(KubernetesJobOperator.template_fields)
    )
    template_fields_renderers = {"env_vars": "json"}

    def __init__(self, *, env_vars, wait_until_job_complete: bool = False, job_poll_interval: float = 10, **kwargs):
        super().__init__(**kwargs)
        self.env_vars = env_vars
        self.wait_until_job_complete = wait_until_job_complete
        self.job_poll_interval = job_poll_interval

    def execute(self, context: Context):
        self.env_vars = convert_env_vars(self.env_vars)
        if context["params"]["environment"] != "localhost":
            self.node_selector = ({"service": "others"},)
            self.tolerations = (
                [k8s.V1Toleration(effect="NoSchedule", key="service", value="others")],
            )

        super().execute(context)

        # WARNING: Remove following lines when Kubernetes provider v8.1.1 is available with Cloud Composer
        if self.wait_until_job_complete:
            self.job = self.wait_until_job_complete(
                job_name=self.job.metadata.name,
                namespace=self.job.metadata.namespace,
                job_poll_interval=self.job_poll_interval,
            )

            context["ti"].xcom_push(key="job", value=self.job.to_dict())

            if error_message := self.is_job_failed(job=self.job):
                raise AirflowException(
                    f"Kubernetes job '{self.job.metadata.name}' is failed with error '{error_message}'"
                )

        # WARNING: Remove function when Kubernetes provider v8.1.1 is available with Cloud Composer    def get_job_status(self, job_name: str, namespace: str) -> k8s.V1Job:
        """Get job with status of specified name and namespace.

        :param job_name: Name of Job to fetch.
        :param namespace: Namespace of the Job.
        :return: Job object
        """
        return self.hook.batch_v1_client.read_namespaced_job_status(
            name=job_name, namespace=namespace, pretty=True
        )

    # WARNING: Remove function when Kubernetes provider v8.1.1 is available with Cloud Composer    def get_job_status(self, job_name: str, namespace: str) -> k8s.V1Job:
    def wait_until_job_complete(self, job_name: str, namespace: str, job_poll_interval: float = 10) -> k8s.V1Job:
        """Block job of specified name and namespace until it is complete or failed.

        :param job_name: Name of Job to fetch.
        :param namespace: Namespace of the Job.
        :param job_poll_interval: Interval in seconds between polling the job status
        :return: Job object
        """
        while True:
            self.log.info("Requesting status for the job '%s' ", job_name)
            job: k8s.V1Job = self.get_job_status(job_name=job_name, namespace=namespace)
            if self.is_job_complete(job=job):
                return job
            self.log.info("The job '%s' is incomplete. Sleeping for %i sec.", job_name, job_poll_interval)
            sleep(job_poll_interval)

    # WARNING: Remove function when Kubernetes provider v8.1.1 is available with Cloud Composer    def get_job_status(self, job_name: str, namespace: str) -> k8s.V1Job:
    @staticmethod
    def is_job_failed(job: k8s.V1Job) -> str | bool:
        """Check whether the given job is failed.

        :return: Error message if the job is failed, and False otherwise.
        """
        if status := job.status:
            conditions = status.conditions or []
            if fail_condition := next((c for c in conditions if c.type == "Failed" and c.status), None):
                return fail_condition.reason
        return False
