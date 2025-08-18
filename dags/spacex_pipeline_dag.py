from airflow.decorators import dag, task
from datetime import datetime

# Import your ingestion functions
from include.launch_ingest import main as ingest_spacex_launches
from include.rocket_ingest import main as ingest_spacex_rockets
from include.pad_ingest import main as ingest_spacex_launchpads
from include.upcoming_ingest import main as ingest_spacex_upcoming
from include.insert_rocket360 import main as insert_into_rocket360

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

@dag(
    dag_id="spacex_launch_ingestion",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["spacex", "pystarburst"],
)
def spacex_pipeline():

    @task
    def fetch_launches():
        ingest_spacex_launches()

    @task
    def fetch_rockets():
        ingest_spacex_rockets()

    @task
    def fetch_launchpads():
        ingest_spacex_launchpads()

    @task
    def fetch_upcoming():
        ingest_spacex_upcoming()

    @task
    def insert_rocket360():
        insert_into_rocket360()


    # Define task dependencies
    l = fetch_launches()
    p = fetch_launchpads()
    r = fetch_rockets()
    u = fetch_upcoming()

    [l, p, r, u] >> insert_rocket360()

# Register the DAG
spacex_pipeline = spacex_pipeline()
