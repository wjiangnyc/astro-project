#!/usr/bin/env python3
"""
SpaceX v4 /rockets → PyStarburst demo.

- GET /v4/rockets
- Coerce arrays/structs for PyStarburst ingestion
- Write to iceberg.spacex.rockets in overwrite mode
"""

import requests
from datetime import datetime
from typing import Any, Dict, List, Tuple
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
    DoubleType as _DoubleType,
)

# -------------------------------
# Schema for rockets
# -------------------------------
def build_v4_rockets_schema() -> _StructType:
    return _StructType([
        _StructField("id", _StringType(), False),
        _StructField("name", _StringType(), True),
        _StructField("type", _StringType(), True),
        _StructField("active", _BooleanType(), True),
        _StructField("stages", _IntegerType(), True),
        _StructField("boosters", _IntegerType(), True),
        _StructField("cost_per_launch", _LongType(), True),
        _StructField("success_rate_pct", _IntegerType(), True),
        _StructField("first_flight", _StringType(), True),
        _StructField("country", _StringType(), True),
        _StructField("company", _StringType(), True),
        _StructField("wikipedia", _StringType(), True),
        _StructField("description", _StringType(), True),
        _StructField("height", _StructType([
            _StructField("meters", _DoubleType(), True),
            _StructField("feet", _DoubleType(), True),
        ]), True),
        _StructField("diameter", _StructType([
            _StructField("meters", _DoubleType(), True),
            _StructField("feet", _DoubleType(), True),
        ]), True),
        _StructField("mass", _StructType([
            _StructField("kg", _LongType(), True),
            _StructField("lb", _LongType(), True),
        ]), True),
        _StructField("payload_weights", _ArrayType(_StructType([
            _StructField("id", _StringType(), True),
            _StructField("name", _StringType(), True),
            _StructField("kg", _LongType(), True),
            _StructField("lb", _LongType(), True),
        ])), True),
    ])

# -------------------------------
# Fetch rockets
# -------------------------------
def fetch_rockets() -> List[Dict[str, Any]]:
    url = "https://api.spacexdata.com/v4/rockets"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        rockets = r.json()
        if not isinstance(rockets, list):
            raise RuntimeError("Unexpected API format: expected a list of rockets")
        print(f"Fetched {len(rockets)} rockets")
        return rockets
    except requests.RequestException as e:
        raise RuntimeError(f"[ERROR] API request failed: {e}")

# -------------------------------
# Coercion helpers
# -------------------------------
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

def _coerce_struct_to_tuple(value: Dict[str, Any], dtype: _StructType) -> Tuple[Any, ...]:
    return tuple(coerce_to_schema(value.get(sf.name), sf.datatype) for sf in dtype.fields)

def coerce_to_schema(value: Any, dtype: Any) -> Any:
    if value is None:
        return None
    if isinstance(dtype, (_StringType, _IntegerType, _LongType, _BooleanType, _DoubleType)):
        return _to_py_primitive(value)
    if isinstance(dtype, _StructType):
        return _coerce_struct_to_tuple(value if isinstance(value, dict) else {}, dtype)
    if isinstance(dtype, _ArrayType):
        return [coerce_to_schema(v, dtype.element_type) for v in value] if isinstance(value, list) else None
    return value

def coerce_records(records: List[Dict[str, Any]], schema: _StructType) -> List[Dict[str, Any]]:
    return [{sf.name: coerce_to_schema(r.get(sf.name), sf.datatype) for sf in schema.fields} for r in records]

# -------------------------------
# Build session
# -------------------------------
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

# -------------------------------
# Main
# -------------------------------
def main():
    rockets = fetch_rockets()
    schema = build_v4_rockets_schema()
    records = coerce_records(rockets, schema)

    session = build_session()
    path_to_table = "iceberg.spacex.rockets"

    try:
        pydf = session.create_dataframe(records, schema=schema)
        pydf.write.mode("overwrite").save_as_table(path_to_table, table_properties={"format": "parquet"})
        print(f"Wrote {len(records)} rockets to {path_to_table}")
    finally:
        session.close()

if __name__ == "__main__":
    main()
