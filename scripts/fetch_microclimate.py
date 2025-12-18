import requests
import pandas as pd
from typing import Any, Dict, List



URL = (
    "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/microclimate-sensors-data/records"
)

locations=['1 Treasury Place',
'101 Collins St L11 Rooftop',
'Batman Park',
'Birrarung Marr Park - Pole 1131',
'CH1 rooftop',
'Enterprize Park - Pole ID: COM1667',
'Royal Park Asset ID: COM2707',
'SkyFarm',
'Swanston St - Tram Stop 13 adjacent Federation Sq & Flinders St Station',
'Tram Stop 7B - Melbourne Tennis Centre Precinct - Rod Laver Arena',
'Tram Stop 7C - Melbourne Tennis Centre Precinct - Rod Laver Arena']

def fetch(url: str = URL, limit=100, location=None, offset=0, timeout: int = 10) -> Dict[str, Any]:
    
    params = {"limit": limit, "offset": offset}
    if location is not None:
        if "SkyFarm" in location:
            params["where"] = f"sensorlocation like '{location}%'"
        else:
            params["where"] = f"sensorlocation like '{location}'"
    resp = requests.get(url, timeout=timeout, params=params)
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
    page_size = 100
    max_total = 1000
    offset = 0
    pages: List[pd.DataFrame] = []

    for location in locations:
        loc_offset = 0
        while loc_offset < max_total:
            j = fetch(limit=page_size, location=location, offset=loc_offset)
            df_page = normalize_records(j)
            if df_page.empty:
                break
            pages.append(df_page)
            fetched = len(df_page)
            print(f"Fetched {fetched} rows for location '{location}' (offset={loc_offset})")
            if fetched < page_size:
                break
            print(f"Fetched {fetched} rows for location '{location}' (offset={loc_offset})")
            loc_offset += page_size
    # while offset < max_total:
    #     j = fetch(limit=page_size, offset=offset)
    #     df_page = normalize_records(j)
    #     if df_page.empty:
    #         break
    #     pages.append(df_page)
    #     fetched = len(df_page)
    #     print(f"Fetched {fetched} rows (offset={offset})")
    #     if fetched < page_size:
    #         break
    #     offset += page_size

    if pages:
        df = pd.concat(pages, ignore_index=True)
    else:
        df = pd.DataFrame()

    print(f"Total rows fetched: {len(df)}; saving to microclimate_20.csv")
    df.to_csv("microclimate_20.csv", index=False)
    if not df.empty:
        print(df.head().to_string())
    return df


if __name__ == "__main__":
    main()
