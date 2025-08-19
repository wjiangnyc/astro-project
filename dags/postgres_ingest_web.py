from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import requests
import psycopg2


default_args = {
    "owner": "wei j",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def fetch_and_insert_launches(**kwargs):
    # Fetch launches from SpaceX API
    url = "https://api.spacexdata.com/v4/launches"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    launches = response.json()

    # Connect to Postgres via Airflow connection
    conn = BaseHook.get_connection("free_neon_hosted")  # Replace this
    conn_params = {
        "host": conn.host,
        "port": conn.port,
        "user": conn.login,
        "password": conn.password,
        "dbname": conn.schema,
        "sslmode": "require"
    }

    connection = psycopg2.connect(**conn_params)
    cursor = connection.cursor()

    insert_sql = """
        INSERT INTO spacex.launches (id, name, date_utc, success)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
    """

    for launch in launches:
        cursor.execute(
            insert_sql,
            (
                launch.get("id"),
                launch.get("name"),
                launch.get("date_utc"),
                launch.get("success")
            )
        )

    connection.commit()
    cursor.close()
    connection.close()
    print(f"Inserted {len(launches)} launches into spacex.launches")

with DAG(
    dag_id="spacex_api_to_postgres_web",
    description="Fetches launches from SpaceX API and inserts into Postgres",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 8, 18),
    catchup=False,
    tags=["spacex", "postgres", "api", "web"],
) as dag:

    create_table_if_missing = SQLExecuteQueryOperator(
        task_id="create_table_if_missing",
        conn_id="free_neon_hosted",  # Replace this
        sql="""
        CREATE TABLE IF NOT EXISTS spacex.launches (
            id TEXT PRIMARY KEY,
            name TEXT,
            date_utc TIMESTAMP,
            success BOOLEAN
        );
        """
    )

    fetch_and_insert = PythonOperator(
        task_id="fetch_and_insert_launches",
        python_callable=fetch_and_insert_launches
        )


    create_table_if_missing >> fetch_and_insert
