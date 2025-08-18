from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "wei j",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="test_neon_postgres_connection",
    description="Test connection to Neon Postgres DB",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 8, 18),
    catchup=False,
    tags=["example", "neon", "postgres"],
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="run_select_now_query",
        conn_id="free_neon",  
        sql='CREATE TABLE IF NOT EXISTS test_table (id INT);'
    )

