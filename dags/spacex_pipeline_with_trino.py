from airflow.decorators import dag, task
from datetime import datetime

# Import your ingestion functions
from include.launch_ingest import main as ingest_spacex_launches
from include.rocket_ingest import main as ingest_spacex_rockets
from include.pad_ingest import main as ingest_spacex_launchpads
from include.upcoming_ingest import main as ingest_spacex_upcoming
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# ============================================================================
# SQL QUERY: Rocket360 Enriched Insert
# ============================================================================

rocket360_sql = """
INSERT INTO iceberg.spacex.rocket360
WITH exploded_cores AS (
  SELECT
    l.id AS launch_id,
    reused
  FROM iceberg.spacex.launches l
  LEFT JOIN UNNEST(l.cores) AS t (
    core,
    flight,
    gridfins,
    legs,
    reused,
    landing_attempt,
    landing_success,
    landing_type,
    landpad
  ) ON TRUE
)
SELECT
  l.id AS launch_id,
  l.name AS launch_name,
  l.rocket AS rocket_id,
  r.name AS rocket_name,
  r.type AS rocket_type,
  r.cost_per_launch,
  r.success_rate_pct,
  l.date_utc,
  l.success,
  l.launchpad AS launchpad_id,
  p.name AS launchpad_name,
  p.locality,
  p.region,
  p.timezone,
  l.upcoming,
  l.details AS launch_details,
  r.description AS rocket_description,
  r.active AS rocket_active,
  c.reused AS rocket_core_reused,
  up.id AS upcoming_id,
  up.date_utc AS upcoming_date_utc,
  up.payloads AS upcoming_payloads,
  up.details AS upcoming_details
FROM iceberg.spacex.launches l
LEFT JOIN iceberg.spacex.rockets r
  ON l.rocket = r.id
LEFT JOIN iceberg.spacex.launchpads p
  ON l.launchpad = p.id
LEFT JOIN exploded_cores c
  ON l.id = c.launch_id
LEFT JOIN iceberg.spacex.upcoming up
  ON l.id = up.id
"""

default_args = {
    "owner": "wei j",
    "start_date": datetime(2024, 8, 18),
    "retries": 5,
}

@dag(
    dag_id="trino_spacex_launch_ingestion",
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

    insert_using_sql = SQLExecuteQueryOperator(
        task_id="insert_rocket360_sql",
        conn_id="wei-starburst",  
        sql=rocket360_sql
    )


    # Define task dependencies
    l = fetch_launches()
    p = fetch_launchpads()
    r = fetch_rockets()
    u = fetch_upcoming()

    [l, p, r, u] >> insert_using_sql

# Register the DAG
spacex_pipeline = spacex_pipeline()
