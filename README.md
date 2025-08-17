# Rocket360: SpaceX Launch Ingestion Pipeline (Airflow DAGs)

This repository contains a set of Apache Airflow DAGs designed to automate the ingestion, transformation, and loading of SpaceX launch data from the public SpaceX API into a Starburst-powered data lake. The resulting datasets power the Rocket360 Dashboard, which provides deep operational insights into SpaceX’s launch program.

---

## Overview

The pipeline:
- Ingests real-time data from SpaceX API endpoints:
  - `/launches`
  - `/launchpads`
  - `/rockets`
  - `/launches/upcoming`
- Transforms and flattens complex nested JSON structures into tabular formats.
- Writes the data into Iceberg tables via PyStarburst (a Python client for Starburst/Trino).
- Enables downstream analytics and dashboarding through Rocket360, a comprehensive analytics dashboard built on top of the data lake.

---

## Rocket360 Dashboard Capabilities

Once the data is ingested and available, the Rocket360 dashboard enables analysis of:

### Launch Metrics
- Total number of launches
- Launches over time
- Launch success vs. failure rates
- Launches broken down by:
  - Rocket type
  - Launchpad
  - Year or quarter

### Financial Metrics
- Total cost of all launches
- Financial losses from failed launches
- ROI estimates based on reuse and success rates
- Projected costs for upcoming launches

### Reusability Insights
- Number of reused rockets
- Reuse frequency by rocket type
- Cost savings from reused boosters

### Planning and Forecasting
- Breakdown of upcoming launches
- Estimated future launch costs
- Distribution by launchpad and rocket type

---

## DAG: `spacex_launch_ingestion`

This DAG contains four parallel tasks, each of which fetches and processes a different SpaceX API endpoint:

| Task ID            | Source Endpoint          | Target Table                    |
|--------------------|--------------------------|----------------------------------|
| `fetch_launches`   | `/v4/launches`           | `iceberg.spacex.launches`       |
| `fetch_launchpads` | `/v4/launchpads`         | `iceberg.spacex.launchpads`     |
| `fetch_rockets`    | `/v4/rockets`            | `iceberg.spacex.rockets`        |
| `fetch_upcoming`   | `/v4/launches/upcoming`  | `iceberg.spacex.upcoming`       |

Each task uses a modular Python ingestion script located in the `include/` directory. These scripts are responsible for:

- Fetching JSON from the SpaceX API
- Flattening nested data structures
- Creating PyStarburst DataFrames
- Saving to Iceberg tables using `overwrite` mode

---