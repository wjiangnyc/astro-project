from airflow.decorators import dag, task
from datetime import datetime

# Import your ingestion functions
from include.launch_ingest import main as ingest_spacex_launches
from include.rocket_ingest import main as ingest_spacex_rockets
from include.pad_ingest import main as ingest_spacex_launchpads
from include.upcoming_ingest import main as ingest_spacex_upcoming

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

    # Run all four in parallel
    fetch_launches()
    fetch_rockets()
    fetch_launchpads()
    fetch_upcoming()

# Register the DAG
spacex_pipeline = spacex_pipeline()
