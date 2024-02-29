import json
import logging

from pathlib import Path

from airflow import settings
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.models import Connection, Param, TaskInstance
from airflow.operators.bash import BashOperator
from airflow.providers.github.hooks.github import GithubHook
from github import GithubException, Repository, UnknownObjectException


for action in ["deploy", "destroy"]:

    @dag(
        dag_id=f"armonik-infra-{action}",
        description=f"Workflow for {action}ing infrastructure of an ArmoniK cluster.",
        schedule=None,
        params={
            "release": Param(
                default="latest", description="ArmoniK release to be deployed.", type="string"
            ),
            "environment": Param(
                default="localhost",
                description="The environment in which to deploy the ArmoniK cluster.",
                type="string",
                enum=["localhost", "aws", "gcp"],
            ),
            "region": Param(
                description="Name of parameter file for infrastructure configuration", type="string"
            ),
            "config": Param(
                description="Name of parameter file for infrastructure configuration", type="string"
            ),
            "armonik_conn_id": Param(
                default="armonik_default",
                description="Reference to an existing or to be created gRPC Connection.",
                type="string",
            ),
            "github_conn_id": Param(
                default="github_default",
                description="Reference to a pre-defined GitHub Connection.",
                type="string",
            ),
            "bucket_prefix": Param(
                default="airflow-bench",
                description="Prefix of the S3/GCS bucket that will store the Terraform state file.",
                type="string",
            ),
        },
    )
    def armonik_dag():
        """Workflow for deploying or updating the infrastructure of an ArmoniK cluster in a given environment for
        a given release and a given configuration. After deployment, an Airflow connection is created or updated
        to enable access to the remote cluster."""

        workdir = Path.cwd()

        @task(task_id="check-release")
        def check_release(ti: TaskInstance, params: dict[str, str]):
            logger = logging.getLogger("airflow.task")
            try:
                # Default method execution is on the top level GitHub client
                logger.info(f"Using Github {params['github_conn_id']} connection.")
                hook = GithubHook(github_conn_id=params["github_conn_id"])
                resource = hook.client

                repo: Repository = resource.get_repo(full_name_or_id="aneoconsulting/ArmoniK")

                match params["release"]:
                    case "main":
                        release = "main"
                    case "latest":
                        release = repo.get_latest_release().tag_name
                    case _:
                        try:
                            release = repo.get_release(params["release"]).tag_name
                        except UnknownObjectException:
                            release = repo.get_branch(params["release"]).name
                logger.info(f"Release is {release}.")
                ti.xcom_push(key="release", value=release)

            except GithubException as github_error:
                raise AirflowException(f"Failed to execute GithubOperator, error: {github_error}")
            except Exception as e:
                raise AirflowException(f"GitHub operator error: {e}")

        check_release = check_release()

        clone_repo = BashOperator(
            task_id="clone-repo",
            bash_command="git clone --depth 1 -b {{ ti.xcom_pull(task_ids='check-release', key='release') }} https://github.com/aneoconsulting/ArmoniK.git",
            cwd=workdir,
        )

        @task(task_id="create-parameters-file")
        def create_parameters_file(params: dict[str, str]) -> None:
            with (
                workdir
                / f"ArmoniK/infrastructure/quick-deploy/{params['environment']}/all-in-one/parameters.tfvars"
            ).open("w") as file:
                file.write(json.dumps(params["config"]))

        create_parameters_file = create_parameters_file()

        delete_cloned_repo = BashOperator(
            task_id="remove-cloned-repo",
            bash_command="rm -rf ArmoniK",
            cwd=workdir,
        )

        if action == "deploy":
            deploy_infra = BashOperator(
                task_id="deplo-infra",
                bash_command="cd ./ArmoniK/infrastructure/quick-deploy/{{ params.environment }}/all-in-one/ && make",
                cwd=workdir,
                env={"PREFIX": "{{ params.bucket_prefix }}", "REGION": "{{ params.region }}"},
                append_env=True,
                trigger_rule="one_success",
            )

            @task(task_id="set-armonik-connection")
            def set_armonik_connection(params: dict[str, str]):
                logger = logging.getLogger("airflow.task")
                output_path = (
                    workdir
                    / f"ArmoniK/infrastructure/quick-deploy/{params['environment']}/all-in-one/generated/armonik-output.json"
                )
                logger.info(f"Reading outputs from {output_path}")
                with output_path.open() as file:
                    outputs = json.loads(file.read())
                    url = outputs["armonik"]["control_plane_url"].removeprefix("http://")
                    if ":" in url:
                        host = url.split(":")[0]
                        port = url.split(":")[1]
                    else:
                        host = url
                        port = None
                logger.info(f"Get host {host} and port {port} from armonik-outputs.json.")
                session = settings.Session()
                try:
                    conn = Connection.get_connection_from_secrets(conn_id=params["armonik_conn_id"])
                    logger.info(
                        f"Connection {params['armonik_conn_id']} alread exists and will be overwritten."
                    )
                except AirflowNotFoundException:
                    conn = Connection(conn_id=params["armonik_conn_id"])
                    logger.info(f"Connection {params['armonik_conn_id']} created.")
                conn.conn_type = "grpc"
                conn.host = host
                conn.port = port
                conn.description = "gRPC connection for a remote ArmoniK cluster"
                conn.extra = json.dumps({"auth_type": "NO_AUTH"})
                session.add(conn)
                session.commit()
                logger.info(f"Connection {params['armonik_conn_id']} added to database.")

            set_armonik_connection = set_armonik_connection()

            (
                check_release
                >> clone_repo
                >> create_parameters_file
                >> deploy_infra
                >> set_armonik_connection
                >> delete_cloned_repo
            )
        elif action == "destroy":
            destroy_infra = BashOperator(
                task_id="destroy-infra",
                bash_command="cd ./ArmoniK/infrastructure/quick-deploy/{{ params.environment }}/all-in-one/ && make get-modules && make destroy",
                cwd=workdir,
                env={
                    "PARAMETERS_FILE": "{{ params.parameters_file }}",
                    "PREFIX": "{{ params.bucket_prefix }}",
                },
                append_env=True,
                trigger_rule="one_success",
            )

            @task(task_id="remove-armonik-connection")
            def remove_armonik_connection(params: dict[str, str]):
                logger = logging.getLogger("airflow.task")
                session = settings.Session()
                try:
                    conn = Connection.get_connection_from_secrets(conn_id=params["armonik_conn_id"])
                    logger.info(
                        f"Connection {params['armonik_conn_id']} exists and will be deleted."
                    )
                    session.delete(conn)
                    session.commit()
                    logger.info(f"Connection {params['armonik_conn_id']} removed from database.")
                except AirflowNotFoundException:
                    logger.info(f"Connection {params['armonik_conn_id']} doesn't exiss.")

            remove_armonik_connection = remove_armonik_connection()

            (
                check_release
                >> clone_repo
                >> create_parameters_file
                >> destroy_infra
                >> [remove_armonik_connection, delete_cloned_repo]
            )

    armonik_dag()
