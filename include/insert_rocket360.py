#!/usr/bin/env python3
"""
Script: insert_rocket360.py
Description:
    This script populates the `iceberg.spacex.rocket360` table by joining data
    from SpaceX's launches, rockets, launchpads, and upcoming launches datasets.
    It leverages PyStarburst to execute a single insert statement.

    The transformation includes:
    - Exploding nested core reuse data
    - Enriching launch data with rocket, pad, and upcoming launch metadata

Author: Your Name
"""

import trino
from pystarburst.session import Session

# ============================================================================
# SQL QUERY: Rocket360 Enriched Insert
# ============================================================================

insert_sql = """
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

# ============================================================================
# Helper: Build PyStarburst session
# ============================================================================

def build_session() -> Session:
    """Establish and return a PyStarburst session."""
    host = "sbe-wei-jiang-lab01.enablement.starburstdata.net"
    db_parameters = {
        "host": host,
        "port": 443,
        "http_scheme": "https",
        "roles": {"system": "ROLE{sysadmin}"},
        "auth": trino.auth.BasicAuthentication("starburst_service", "StarburstR0cks!")
    }

    try:
        return Session.builder.configs(db_parameters).create()
    except Exception as e:
        raise RuntimeError(f"[ERROR] Failed to create PyStarburst session: {e}")

# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Main entry point: run the insert statement into rocket360."""
    session = build_session()

    try:
        # Execute the INSERT INTO ... SELECT statement
        session.sql(insert_sql).collect()
        print("[INFO] rocket360 table insert completed successfully.")
    except Exception as e:
        print(f"[ERROR] SQL execution failed: {e}")
    finally:
        # Clean up the session
        session.close()

# ============================================================================
# Entrypoint
# ============================================================================

if __name__ == "__main__":
    main()
