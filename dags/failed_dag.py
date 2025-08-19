from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "wei j",
    "retries": 5,
    'retry_delay': timedelta(seconds=5),  # Set retry delay to 5 seconds
}

with DAG(
    dag_id="failed_dag",
    description="creating a dag that fails",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 8, 18),
    catchup=False,
    tags=["example", "neon", "postgres"],
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="run_select_now_query",
        conn_id="wrong conn",  
        sql='CREATE TABLE IF NOT EXISTS test_table (id INT);'
    )

