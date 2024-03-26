import json
import yaml

from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.connection import Connection
from airflow.hooks.base import BaseHook
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow import settings


@dag(dag_id="test")
def test_dag():
    @task
    def create_kube_conn(**context):
        conn = Connection(
            conn_id="kube_testjs5",
            conn_type="kubernetes",
            extra={"kube_config": json.dumps(yaml.safe_load((Path.cwd() / "kubeconfig").open().read()))},
        )
        session = settings.Session()
        session.add(conn)
        session.commit()
        hook = KubernetesHook(conn_id="kube_testjs5")
        import pdb; pdb.set_trace()
            

    create_kube_conn()


test_dag()
