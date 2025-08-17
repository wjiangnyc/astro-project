#!/usr/bin/env python3
"""
Fetch upcoming SpaceX launches → Write to iceberg.spacex.upcoming
"""

import requests
import trino
from datetime import datetime
from typing import List, Dict, Any
from pystarburst.session import Session
from pystarburst.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, BooleanType, TimestampType,
    ArrayType
)


# -----------------------------
# Schema for upcoming launches
# -----------------------------
def build_upcoming_schema() -> StructType:
    return StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("flight_number", IntegerType(), True),
        StructField("date_utc", TimestampType(), True),
        StructField("date_unix", LongType(), True),
        StructField("date_local", TimestampType(), True),
        StructField("date_precision", StringType(), True),
        StructField("upcoming", BooleanType(), True),
        StructField("rocket", StringType(), True),
        StructField("launchpad", StringType(), True),
        StructField("success", BooleanType(), True),
        StructField("details", StringType(), True),
        StructField("crew", ArrayType(StringType()), True),
        StructField("ships", ArrayType(StringType()), True),
        StructField("capsules", ArrayType(StringType()), True),
        StructField("payloads", ArrayType(StringType()), True)
    ])


# -----------------------------
# API Fetch Function
# -----------------------------
def fetch_upcoming_launches() -> List[Dict[str, Any]]:
    url = "https://api.spacexdata.com/v4/launches/upcoming"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[ERROR] Failed to fetch upcoming launches: {e}")
        return []


# -----------------------------
# Session Builder
# -----------------------------
def build_session() -> Session | None:
    db_parameters = {
        "host": "sbe-wei-jiang-lab01.enablement.starburstdata.net",
        "port": 443,
        "http_scheme": "https",
        "roles": {"system": "ROLE{sysadmin}"},
        "auth": trino.auth.BasicAuthentication("starburst_service", "StarburstR0cks!")
    }
    try:
        return Session.builder.configs(db_parameters).create()
    except Exception as e:
        print(f"[ERROR] Failed to create PyStarburst session: {e}")
        return None


# -----------------------------
# Coercion Helper
# -----------------------------
def parse_datetime(val):
    if not val:
        return None
    try:
        return datetime.fromisoformat(val.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def coerce_records(records: List[Dict[str, Any]], schema: StructType) -> List[Dict[str, Any]]:
    out = []
    for r in records:
        row = {}
        for sf in schema.fields:
            val = r.get(sf.name)
            if isinstance(sf.datatype, TimestampType):
                val = parse_datetime(val)
            row[sf.name] = val
        out.append(row)
    return out


# -----------------------------
# Main
# -----------------------------
def main():
    launches = fetch_upcoming_launches()
    if not launches:
        print("[INFO] No upcoming launches found or fetch failed.")
        return

    session = build_session()
    if not session:
        return

    schema = build_upcoming_schema()
    records = coerce_records(launches, schema)

    try:
        pydf = session.create_dataframe(records, schema=schema)
        pydf.write.mode("overwrite").save_as_table(
            "iceberg.spacex.upcoming",
            table_properties={"format": "parquet"}
        )
        print(f"[SUCCESS] Wrote {len(records)} upcoming launches to iceberg.spacex.upcoming")
    except Exception as e:
        print(f"[ERROR] Failed to write table: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
