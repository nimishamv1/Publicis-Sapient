import requests
import pandas as pd
from typing import Any, Dict, List

URL = (
    "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/microclimate-sensors-data/records?limit=20"
)


def fetch(url: str = URL, timeout: int = 10) -> Dict[str, Any]:
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def normalize_records(j: Any) -> pd.DataFrame:
    # Locate the list of record-like objects in the JSON response
    candidates = None
    if isinstance(j, dict):
        if "results" in j:
            candidates = j["results"]
        elif "records" in j:
            candidates = j["records"]
        elif "data" in j and isinstance(j["data"], list):
            candidates = j["data"]
    if candidates is None and isinstance(j, list):
        candidates = j
    if candidates is None:
        return pd.DataFrame([j])

    rows: List[Dict[str, Any]] = []
    for r in candidates:
        if not isinstance(r, dict):
            rows.append({"value": r})
            continue

        # Common shapes:
        # - { "record": { "fields": {...} } }
        # - { "fields": {...} }
        # - { ...fields directly... }
        if "record" in r:
            rec = r["record"]
            if isinstance(rec, dict) and "fields" in rec:
                rows.append(rec["fields"])
            elif isinstance(rec, dict):
                rows.append(rec)
            else:
                rows.append({"value": rec})
        elif "fields" in r:
            rows.append(r["fields"])
        else:
            rows.append(r)

    return pd.DataFrame(rows)


def main() -> pd.DataFrame:
    print("Fetching data from API...")
    j = fetch()
    df = normalize_records(j)
    print(f"Fetched {len(df)} rows; saving to microclimate_20.csv")
    df.to_csv("microclimate_20.csv", index=False)
    print(df.head().to_string())
    print("hello")
    return df


if __name__ == "__main__":
    main()
