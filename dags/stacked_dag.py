from airflow.decorators import dag, task

default_args = {
    "owner": "wei j",
}

@dag(
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="stacked dag with tasks running in parallel",
    tags=["example"],
)
def stacked_dag():

    @task
    def task1():
        print("this")

    @task
    def task2():
        print("dag")

    @task
    def task3():
        print("is")

    @task
    def task4():
        print("stacked")

    # Chain the tasks together
    task1()
    task2()
    task3()
    task4()

# Instantiate the DAG
stacked_dag()
