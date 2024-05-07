from typing import Any, Sequence

from airflow.models.baseoperator import BaseOperator
from airflow.models.connection import Connection
from airflow.utils.context import Context
from airflow.utils.session import create_session
from sqlalchemy import select


class UpdateAirflowConnectionOperator(BaseOperator):
    """
    Create or update a connection in the Airflow database.

    Args:
        conn_id (str): The connection ID.
        conn_type (str): The connection type.
        description (str): The connection description.
        host (str): The host.
        login (str): The login.
        password (str): The password.
        schema (str): The schema.
        port (int): The port number.
        extra (str): Extra metadata. Non-standard data such as private/SSH keys can be saved here. JSON
            encoded object.
        uri (str): URI address describing connection parameters.
    """

    template_fields: Sequence[str] = (
        "conn_id",
        "conn_type",
        "description",
        "host",
        "login",
        "password",
        "schema",
        "port",
        "extra",
        "uri",
    )

    def __init__(
        self,
        conn_id: str | None = None,
        conn_type: str | None = None,
        description: str | None = None,
        host: str | None = None,
        login: str | None = None,
        password: str | None = None,
        schema: str | None = None,
        port: int | None = None,
        extra: str | dict | None = None,
        uri: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.conn_type = conn_type
        self.description = description
        self.host = host
        self.login = login
        self.password = password
        self.schema = schema
        self.port = port
        self.extra = extra
        self.uri = uri

    def execute(self, context: Context) -> Any:
        with create_session() as session:
            self.log.info(f"connection id: {self.conn_id}")
            conn: Connection = session.scalar(
                select(Connection).where(Connection.conn_id == self.conn_id)
            )
            if conn:
                self.log.info("connection already exists")
                session.delete(conn)
                session.commit()
                self.log.info("already existing connection deleted")
            conn = Connection(
                conn_id=self.conn_id,
                conn_type=self.conn_type,
                description=self.description,
                host=self.host,
                login=self.login,
                password=self.password,
                schema=self.schema,
                port=self.port,
                extra=self.extra,
                uri=self.uri,
            )
            session.add(conn)
            session.commit()
            self.log.info(f"created new connection: {conn}")
