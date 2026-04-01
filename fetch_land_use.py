"""
fetch_land_use.py
Uses OSM Nominatim reverse geocoding to classify each top-100 OTA site
as: rural, industrial, urban, or unknown.

Run: python3 fetch_land_use.py
Output: site_land_use.json
"""

import json
import time
from pathlib import Path

import pandas as pd
import requests

EXCEL_PATH   = Path("data/TOP 100 OTA Sites ARN 20260331.xlsx")
OUTPUT_PATH  = Path("site_land_use.json")
NOMINATIM    = "https://nominatim.openstreetmap.org/reverse"
HEADERS      = {"User-Agent": "optimus-data-landuse/1.0 (anna@conductor.energy)"}

# Address keys in the Nominatim response that signal each category
INDUSTRIAL_KEYS = {"industrial"}
RURAL_KEYS      = {"village", "hamlet", "farm", "farmland", "forest",
                   "nature_reserve", "peak", "valley", "locality"}
URBAN_KEYS      = {"city", "suburb", "city_district", "borough", "quarter", "town"}


def classify(address: dict) -> str:
    keys = set(address.keys())
    if keys & INDUSTRIAL_KEYS:
        return "industrial"
    if keys & RURAL_KEYS and not (keys & URBAN_KEYS):
        return "rural"
    if keys & URBAN_KEYS:
        return "urban"
    return "unknown"


def reverse_geocode(lat: float, lon: float) -> dict:
    for attempt in range(3):
        try:
            resp = requests.get(
                NOMINATIM,
                params={"lat": lat, "lon": lon, "format": "json", "zoom": 16},
                headers=HEADERS,
                timeout=15,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"  Error (attempt {attempt + 1}): {e}")
            time.sleep(3)
    return {}


def main() -> None:
    df = pd.read_excel(EXCEL_PATH, engine="openpyxl")
    col_lower = {c.strip().lower(): c for c in df.columns}
    df = df.rename(columns={
        col_lower["site id"]:   "site_id",
        col_lower["latitude"]:  "lat",
        col_lower["longitude"]: "lon",
    })
    df["site_id"] = df["site_id"].astype(str)

    results = []
    total = len(df)
    for i, row in df.iterrows():
        sid = row["site_id"]
        lat = float(row["lat"])
        lon = float(row["lon"])

        data     = reverse_geocode(lat, lon)
        address  = data.get("address", {})
        land_use = classify(address)
        is_non_urban = land_use in ("rural", "industrial")

        print(f"[{i+1}/{total}] {sid} → {land_use}  "
              f"(keys: {list(address.keys())[:6]})")
        results.append({
            "site_id":      sid,
            "land_use":     land_use,
            "is_non_urban": is_non_urban,
            "address_keys": list(address.keys()),
        })
        time.sleep(1)  # Nominatim rate limit: max 1 req/sec

    OUTPUT_PATH.write_text(json.dumps(results, indent=2, ensure_ascii=False))
    summary = {k: sum(1 for r in results if r["land_use"] == k)
               for k in ("rural", "industrial", "urban", "unknown")}
    print(f"\nDone → {OUTPUT_PATH}  |  {summary}")


if __name__ == "__main__":
    main()
