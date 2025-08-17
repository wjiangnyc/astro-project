#!/usr/bin/env python3
"""
SpaceX v4 /launches → PyStarburst demo (schema-coerced, ISO8601→datetime).

- GET /v4/launches/query (paginated)
- Coerce Python objects to match a StructType schema
- Create PyStarburst DF
- Write once at the end (no partial ingestion)
"""

from typing import List, Dict, Any, Tuple
from datetime import datetime
import requests
import trino

from pystarburst.session import Session
from pystarburst.types import (
    StructType as _StructType,
    StructField as _StructField,
    ArrayType as _ArrayType,
    StringType as _StringType,
    IntegerType as _IntegerType,
    LongType as _LongType,
    BooleanType as _BooleanType,
    TimestampType as _TimestampType,
)

# ==============================
# Schema for /v4/launches
# ==============================
def build_v4_launch_schema() -> _StructType:
    return _StructType([
        _StructField("net", _BooleanType(), True),
        _StructField("window", _IntegerType(), True),
        _StructField("rocket", _StringType(), True),
        _StructField("success", _BooleanType(), True),
        _StructField("details", _StringType(), True),
        _StructField("launchpad", _StringType(), True),
        _StructField("flight_number", _IntegerType(), True),
        _StructField("name", _StringType(), True),
        _StructField("date_utc", _TimestampType(), True),
        _StructField("date_unix", _LongType(), True),
        _StructField("date_local", _TimestampType(), True),
        _StructField("date_precision", _StringType(), True),
        _StructField("upcoming", _BooleanType(), True),
        _StructField("auto_update", _BooleanType(), True),
        _StructField("tbd", _BooleanType(), True),
        _StructField("launch_library_id", _StringType(), True),
        _StructField("id", _StringType(), False),
        _StructField("crew", _ArrayType(_StringType()), True),
        _StructField("ships", _ArrayType(_StringType()), True),
        _StructField("capsules", _ArrayType(_StringType()), True),
        _StructField("payloads", _ArrayType(_StringType()), True),
        _StructField("fairings", _StructType([
            _StructField("reused", _BooleanType(), True),
            _StructField("recovery_attempt", _BooleanType(), True),
            _StructField("recovered", _BooleanType(), True),
            _StructField("ships", _ArrayType(_StringType()), True),
        ]), True),
        _StructField("links", _StructType([
            _StructField("patch", _StructType([
                _StructField("small", _StringType(), True),
                _StructField("large", _StringType(), True),
            ]), True),
            _StructField("reddit", _StructType([
                _StructField("campaign", _StringType(), True),
                _StructField("launch", _StringType(), True),
                _StructField("media", _StringType(), True),
                _StructField("recovery", _StringType(), True),
            ]), True),
            _StructField("flickr", _StructType([
                _StructField("small", _ArrayType(_StringType()), True),
                _StructField("original", _ArrayType(_StringType()), True),
            ]), True),
            _StructField("presskit", _StringType(), True),
            _StructField("webcast", _StringType(), True),
            _StructField("youtube_id", _StringType(), True),
            _StructField("article", _StringType(), True),
            _StructField("wikipedia", _StringType(), True),
        ]), True),
        _StructField("failures", _ArrayType(_StructType([
            _StructField("time", _IntegerType(), True),
            _StructField("altitude", _IntegerType(), True),
            _StructField("reason", _StringType(), True),
        ])), True),
        _StructField("cores", _ArrayType(_StructType([
            _StructField("core", _StringType(), True),
            _StructField("flight", _IntegerType(), True),
            _StructField("gridfins", _BooleanType(), True),
            _StructField("legs", _BooleanType(), True),
            _StructField("reused", _BooleanType(), True),
            _StructField("landing_attempt", _BooleanType(), True),
            _StructField("landing_success", _BooleanType(), True),
            _StructField("landing_type", _StringType(), True),
            _StructField("landpad", _StringType(), True),
        ])), True),
        _StructField("static_fire_date_utc", _TimestampType(), True),
        _StructField("static_fire_date_unix", _LongType(), True),
    ])

# ==============================
# Fetch with pagination
# ==============================
def fetch_all_launches_paginated(limit: int = 200) -> List[Dict[str, Any]]:
    url = "https://api.spacexdata.com/v4/launches/query"
    all_launches = []
    page = 1

    while True:
        try:
            r = requests.post(
                url,
                json={"query": {}, "options": {"limit": limit, "page": page}},
                timeout=30
            )
            r.raise_for_status()
            data = r.json()

            if "docs" not in data:
                raise RuntimeError(f"Unexpected API structure on page {page}: {data}")

            launches_page = data["docs"]
            if not launches_page:
                break

            all_launches.extend(launches_page)
            print(f"Fetched page {page}, {len(launches_page)} launches")

            total_pages = data.get("totalPages")
            if total_pages and page >= total_pages:
                break

            page += 1

        except requests.RequestException as e:
            raise RuntimeError(f"API request failed on page {page}: {e}")

    print(f"Total launches fetched: {len(all_launches)}")
    return all_launches

# ==============================
# Coercion helpers
# ==============================
def _to_py_primitive(x: Any) -> Any:
    try:
        import numpy as np
        if isinstance(x, np.integer):
            return int(x)
        if isinstance(x, np.floating):
            return None if np.isnan(x) else float(x)
        if isinstance(x, np.bool_):
            return bool(x)
    except Exception:
        pass
    return x

def _parse_iso_to_naive_datetime(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.replace(tzinfo=None)
    if isinstance(val, str):
        s = val.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(s).replace(tzinfo=None)
        except Exception:
            return None
    return None

def _coerce_struct_to_tuple(value: Dict[str, Any], dtype: _StructType) -> Tuple[Any, ...]:
    return tuple(coerce_to_schema(value.get(sf.name), sf.datatype) for sf in dtype.fields)

def coerce_to_schema(value: Any, dtype: Any) -> Any:
    if value is None:
        return None
    if isinstance(dtype, _TimestampType):
        return _parse_iso_to_naive_datetime(value)
    if isinstance(dtype, (_StringType, _IntegerType, _LongType, _BooleanType)):
        return _to_py_primitive(value)
    if isinstance(dtype, _StructType):
        return _coerce_struct_to_tuple(value if isinstance(value, dict) else {}, dtype)
    if isinstance(dtype, _ArrayType):
        return [coerce_to_schema(v, dtype.element_type) for v in value] if isinstance(value, list) else None
    return value

def coerce_records(records: List[Dict[str, Any]], schema: _StructType) -> List[Dict[str, Any]]:
    return [{sf.name: coerce_to_schema(r.get(sf.name), sf.datatype) for sf in schema.fields} for r in records]

# ==============================
# Build session
# ==============================
def build_session() -> Session:
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

# ==============================
# Main
# ==============================
def main():
    launches = fetch_all_launches_paginated()
    schema = build_v4_launch_schema()
    records = coerce_records(launches, schema)

    session = build_session()
    pydf = session.create_dataframe(records, schema=schema)

    path_to_table = "iceberg.spacex.launches"
    pydf.write.mode("append").save_as_table(path_to_table, table_properties={"format": "parquet"})

    print(f"Wrote {len(records)} launches to {path_to_table}")
    session.close()

if __name__ == "__main__":
    main()
