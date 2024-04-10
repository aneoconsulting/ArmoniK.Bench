from airflow.models.connection import Connection
from airflow.utils.session import NEW_SESSION, provide_session
from sqlalchemy import select
from sqlalchemy.orm.session import Session

from src.airflow.dags.operators.connection import (
    CreateOrUpdateConnectionOperator,
    DeleteConnectionOperator,
)


@provide_session
def test_create_or_update_connection_operator_no_existing_connection(
    mocker, session: Session = NEW_SESSION
) -> None:
    conn_id = "test1_default"

    conn: Connection = session.scalar(select(Connection).where(Connection.conn_id == conn_id))
    assert not conn

    op = CreateOrUpdateConnectionOperator(
        task_id="test",
        conn_id=conn_id,
        conn_type="test",
        description="test connection",
        host="host",
        port=10,
    )
    op.execute(mocker.Mock())

    conn: Connection = session.scalar(select(Connection).where(Connection.conn_id == conn_id))
    assert conn
    assert conn.host == "host"
    assert conn.port == 10

    session.delete(conn)
    session.commit()


@provide_session
def test_create_or_update_connection_operator_with_existing_connection(
    mocker, session: Session = NEW_SESSION
) -> None:
    conn_id = "test2_default"

    conn = Connection(conn_id=conn_id, conn_type="test")
    session.add(conn)
    session.commit()

    op = CreateOrUpdateConnectionOperator(
        task_id="test",
        conn_id=conn_id,
        conn_type="test",
        description="test connection",
        host="host",
        port=10,
    )
    op.execute(mocker.Mock())

    conn: Connection = session.scalar(select(Connection).where(Connection.conn_id == conn_id))
    assert conn
    assert conn.host == "host"
    assert conn.port == 10

    session.delete(conn)
    session.commit()


@provide_session
def test_delete_connection_operator(mocker, session: Session = NEW_SESSION) -> None:
    conn_id = "test3_default"

    conn = Connection(conn_id=conn_id, conn_type="test")
    session.add(conn)
    session.commit()

    conn: Connection = session.scalar(select(Connection).where(Connection.conn_id == conn_id))
    assert conn

    op = DeleteConnectionOperator(task_id="test", conn_ids=[conn_id, "nothing"])
    op.execute(mocker.Mock())

    conn: Connection = session.scalar(select(Connection).where(Connection.conn_id == conn_id))
    assert not conn
