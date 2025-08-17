#!/usr/bin/env python3
"""
SpaceX v4 /launchpads → PyStarburst (clean version with no CLI args)
"""

from __future__ import annotations
import json
from typing import Any, Dict, List

import requests
import pandas as pd
import trino
from pystarburst.session import Session

LAUNCHPADS_URL = "https://api.spacexdata.com/v4/launchpads"

# ---------------------------
# Helpers
# ---------------------------
def fetch_json(url: str) -> Any:
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[ERROR] Failed to fetch {url}: {e}")
        return None

def stringify_nested(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return value

def flatten_launchpad(lp: Dict[str, Any]) -> Dict[str, Any]:
    return {k: stringify_nested(v) for k, v in lp.items()}

def build_session() -> Session:
    host = "sbe-wei-jiang-lab01.enablement.starburstdata.net"
    db_parameters = {
        "host": host,
        "port": 443,
        "http_scheme": "https",
        "roles": {"system": "ROLE{sysadmin}"},
        "auth": trino.auth.BasicAuthentication("starburst_service", "StarburstR0cks!")
    }
    return Session.builder.configs(db_parameters).create()

# ---------------------------
# Main
# ---------------------------
def main():
    # 1) Fetch
    launchpads = fetch_json(LAUNCHPADS_URL)
    if launchpads is None or not isinstance(launchpads, list):
        print("[ERROR] Launchpad API did not return a valid list.")
        return
    print(f"Fetched {len(launchpads)} launchpads")

    # 2) Flatten
    try:
        records: List[Dict[str, Any]] = [flatten_launchpad(lp) for lp in launchpads]
    except Exception as e:
        print(f"[ERROR] Failed to flatten launchpads: {e}")
        return

    # 3) Pandas preview
    try:
        df = pd.DataFrame(records).where(lambda x: x.notna(), None)
        print(df.head(3))
    except Exception as e:
        print(f"[ERROR] Failed to create Pandas DataFrame: {e}")
        return

    # 4) PyStarburst write
    session = None
    try:
        session = build_session()
        pydf = session.create_dataframe(df.to_dict(orient="records"))
        path_to_table = "iceberg.spacex.launchpads"
        pydf.write.mode("overwrite").save_as_table(
            path_to_table, table_properties={"format": "parquet"}
        )
        print(f"Saved to table: {path_to_table}")
    except Exception as e:
        print(f"[ERROR] PyStarburst step failed: {e}")
    finally:
        try:
            if session:
                session.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
