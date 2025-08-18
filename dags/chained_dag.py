from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "wei j",
}

@dag(
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="Chained DAG that builds a sentence word by word",
    tags=["example"],
)
def chained_dag():

    @task
    def task1():
        return "this"

    @task
    def task2(prev: str):
        return f"{prev} is"

    @task
    def task3(prev: str):
        return f"{prev} a"

    @task
    def task4(prev: str):
        return f"{prev} chained"

    @task
    def build_sentence(prev: str):
        return f"{prev} dag"

    w1 = task1()
    w2 = task2(w1)
    w3 = task3(w2)
    w4 = task4(w3)
    final = build_sentence(w4)

    print_to_logs = BashOperator(
        task_id="print_to_command_line",
        bash_command='echo "{{ ti.xcom_pull(task_ids=\'build_sentence\') }}"'
    )

    final >> print_to_logs

# Instantiate the DAG
chained_dag()
