"""
fetch_land_use.py
Uses the Overpass API to check for residential buildings and residential
landuse zones within 100 m of each top-100 OTA site.

Why 100 m?  Battery storage units produce ~75 dB at 1 m.  Sound drops
~6 dB per doubling of distance, reaching ~35 dB (ambient) at ~100 m.

Classification:
  - noise_safe          → no residential features within 100 m
  - residential_nearby  → residential buildings or landuse within 100 m

Run:  python3 fetch_land_use.py
Output: site_land_use.json
"""

import json
import time
from pathlib import Path

import pandas as pd
import requests

EXCEL_PATH  = Path("data/TOP 100 OTA Sites ARN 20260331.xlsx")
OUTPUT_PATH = Path("site_land_use.json")
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
HEADERS = {"User-Agent": "optimus-data-landuse/1.0 (anna@conductor.energy)"}

RADIUS_M = 100  # buffer around each site in metres

# Overpass QL: find residential features within RADIUS_M of a point.
# Covers:  landuse=residential polygons  +  residential-type buildings
OVERPASS_QUERY = """
[out:json][timeout:30];
(
  way["landuse"="residential"](around:{radius},{lat},{lon});
  relation["landuse"="residential"](around:{radius},{lat},{lon});
  way["building"~"^(residential|apartments|house|detached|semidetached_house|terrace|dormitory)$"](around:{radius},{lat},{lon});
  node["building"~"^(residential|apartments|house|detached|semidetached_house|terrace|dormitory)$"](around:{radius},{lat},{lon});
);
out count;
"""

# Secondary query: what landuse polygons actually contain the point?
LANDUSE_QUERY = """
[out:json][timeout:30];
is_in({lat},{lon})->.a;
area.a["landuse"]->.b;
.b out tags;
"""


def query_overpass(query: str, lat: float, lon: float) -> dict:
    """Send a query to Overpass and return the JSON response."""
    q = query.format(radius=RADIUS_M, lat=lat, lon=lon)
    for attempt in range(3):
        try:
            resp = requests.post(
                OVERPASS_URL,
                data={"data": q},
                headers=HEADERS,
                timeout=60,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"  Overpass error (attempt {attempt + 1}): {e}")
            time.sleep(5 * (attempt + 1))
    return {}


def classify_site(lat: float, lon: float) -> dict:
    """Return residential count and surrounding landuse for one site."""
    # 1) Count residential features within radius
    res = query_overpass(OVERPASS_QUERY, lat, lon)
    res_count = 0
    if "elements" in res:
        for el in res["elements"]:
            if el.get("type") == "count":
                res_count = el.get("tags", {}).get("total", 0)
                res_count = int(res_count)
                break

    # 2) What landuse zone(s) is the point actually inside?
    landuse_res = query_overpass(LANDUSE_QUERY, lat, lon)
    landuse_tags = []
    for el in landuse_res.get("elements", []):
        lu = el.get("tags", {}).get("landuse")
        if lu:
            landuse_tags.append(lu)

    noise_safe = res_count == 0
    # Map to a human-readable context string
    if landuse_tags:
        nearby_landuse = ", ".join(sorted(set(landuse_tags)))
    else:
        nearby_landuse = "unknown"

    return {
        "noise_safe": noise_safe,
        "residential_count": res_count,
        "nearby_landuse": nearby_landuse,
    }


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

        info = classify_site(lat, lon)
        label = "noise_safe" if info["noise_safe"] else "residential_nearby"
        print(f"[{i+1}/{total}] {sid} → {label}  "
              f"(residential={info['residential_count']}, "
              f"landuse={info['nearby_landuse']})")

        results.append({
            "site_id":           sid,
            "noise_safe":        info["noise_safe"],
            "residential_count": info["residential_count"],
            "nearby_landuse":    info["nearby_landuse"],
            # Backwards-compat: map to the old land_use field used by inject
            "land_use":          "noise_safe" if info["noise_safe"] else "residential_nearby",
        })
        # Be nice to the public Overpass server
        time.sleep(2)

    OUTPUT_PATH.write_text(json.dumps(results, indent=2, ensure_ascii=False))
    safe = sum(1 for r in results if r["noise_safe"])
    print(f"\nDone → {OUTPUT_PATH}")
    print(f"  Noise-safe: {safe}/{total}  |  Near residential: {total - safe}/{total}")


if __name__ == "__main__":
    main()
