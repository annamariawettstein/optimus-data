"""
fetch_weather_risk.py
Pre-compute 10-year historical weather risk for each OTA site.

Usage:
    pip install requests pandas openpyxl
    python fetch_weather_risk.py
"""

import json
import logging
import math
import time
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

# ── Constants ────────────────────────────────────────────────────────────────
EXCEL_PATH  = Path("data/TOP 100 OTA Sites ARN 20260327.xlsx")
OUTPUT_PATH = Path("site_weather_risk.json")
API_BASE    = "https://archive-api.open-meteo.com/v1/archive"
START_DATE  = "2015-01-01"
END_DATE    = "2024-12-31"
DAILY_VARS  = "precipitation_sum,snowfall_sum,windspeed_10m_max,weathercode"

THUNDERSTORM_CODES = {95, 96, 99}
SNOW_THRESHOLD = 20.0   # cm
WIND_THRESHOLD = 60.0   # km/h
RAIN_THRESHOLD = 30.0   # mm

WEIGHTS = {"thunderstorm": 0.40, "wind": 0.25, "snow": 0.20, "rain": 0.15}

REQUEST_DELAY = 2.0  # seconds between API calls
MAX_RETRIES   = 4

REQUIRED_COLS = ["site id", "latitude", "longitude", "region", "current no. of tenants"]
COL_MAP = {
    "site id":                "site_id",
    "latitude":               "lat",
    "longitude":              "lon",
    "region":                 "region",
    "current no. of tenants": "tenants",
}

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


# ── Excel reader ─────────────────────────────────────────────────────────────
def read_sites(path: Path) -> List[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Excel file not found: {path}")

    df = pd.read_excel(path, engine="openpyxl")

    # Case-insensitive column resolution
    col_lower = {c.strip().lower(): c for c in df.columns}
    missing = [r for r in REQUIRED_COLS if r not in col_lower]
    if missing:
        raise KeyError(
            f"Required columns not found: {missing}\n"
            f"Found columns: {list(df.columns)}"
        )

    # Rename to canonical names
    rename = {col_lower[r]: COL_MAP[r] for r in REQUIRED_COLS}
    df = df.rename(columns=rename)
    df = df[list(COL_MAP.values())]

    # Drop duplicate site IDs
    before = len(df)
    df = df.drop_duplicates(subset="site_id", keep="first")
    if len(df) < before:
        logger.warning("Dropped %d duplicate site ID(s)", before - len(df))

    sites = []
    for _, row in df.iterrows():
        if pd.isna(row["lat"]) or pd.isna(row["lon"]):
            logger.warning("Skipping site %s — missing lat/lon", row["site_id"])
            continue
        tenants = None if pd.isna(row["tenants"]) else int(row["tenants"])
        sites.append({
            "site_id": str(row["site_id"]),
            "lat":     float(row["lat"]),
            "lon":     float(row["lon"]),
            "region":  str(row["region"]) if not pd.isna(row["region"]) else "",
            "tenants": tenants,
        })
    return sites


# ── API fetcher ───────────────────────────────────────────────────────────────
def fetch_site_weather(site_id: str, lat: float, lon: float) -> Optional[dict]:
    url = (
        f"{API_BASE}"
        f"?latitude={lat:.4f}&longitude={lon:.4f}"
        f"&start_date={START_DATE}&end_date={END_DATE}"
        f"&daily={DAILY_VARS}&timezone=auto"
    )
    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                # Rate limited — back off exponentially
                backoff = REQUEST_DELAY * (2 ** (attempt + 1))
                logger.warning("Site %s: HTTP 429 rate-limited (attempt %d/%d) — waiting %.0fs", site_id, attempt + 1, MAX_RETRIES + 1, backoff)
                time.sleep(backoff)
                continue
            if 400 <= resp.status_code < 500:
                logger.error("Site %s: HTTP %d — skipping", site_id, resp.status_code)
                return None
            # 5xx → retry
            logger.warning("Site %s: HTTP %d (attempt %d/%d)", site_id, resp.status_code, attempt + 1, MAX_RETRIES + 1)
        except requests.ConnectionError as e:
            logger.warning("Site %s: connection error (attempt %d/%d): %s", site_id, attempt + 1, MAX_RETRIES + 1, e)
        if attempt < MAX_RETRIES:
            time.sleep(REQUEST_DELAY * 2)
    logger.error("Site %s: all retries exhausted", site_id)
    return None


# ── Metric aggregator ─────────────────────────────────────────────────────────
def aggregate_metrics(daily: dict) -> dict:
    times      = daily.get("time", [])
    precip     = daily.get("precipitation_sum", [])
    snowfall   = daily.get("snowfall_sum", [])
    windspeed  = daily.get("windspeed_10m_max", [])
    wxcodes    = daily.get("weathercode", [])

    # Group by calendar year
    year_data: Dict[int, dict] = {}
    for i, t in enumerate(times):
        year = int(t[:4])
        if year not in year_data:
            year_data[year] = {"count": 0, "thunder": 0, "snow": 0, "wind": 0, "rain": 0}
        d = year_data[year]
        d["count"] += 1

        wc   = wxcodes[i]   if i < len(wxcodes)   else None
        rain = precip[i]    if i < len(precip)     else None
        snow = snowfall[i]  if i < len(snowfall)   else None
        wind = windspeed[i] if i < len(windspeed)  else None

        # Treat None/null as 0 for threshold comparisons (conservative)
        wc_val   = 0 if wc   is None else int(wc)
        rain_val = 0.0 if rain is None else float(rain)
        snow_val = 0.0 if snow is None else float(snow)
        wind_val = 0.0 if wind is None else float(wind)

        d["thunder"] += 1 if wc_val in THUNDERSTORM_CODES else 0
        d["snow"]    += 1 if snow_val >= SNOW_THRESHOLD else 0
        d["wind"]    += 1 if wind_val >= WIND_THRESHOLD else 0
        d["rain"]    += 1 if rain_val >= RAIN_THRESHOLD else 0

    # Only average over years with ≥ 300 data points
    valid_years = [d for d in year_data.values() if d["count"] >= 300]
    if not valid_years:
        return {
            "avg_thunderstorm_days": None,
            "avg_heavy_snow_days":   None,
            "avg_high_wind_days":    None,
            "avg_heavy_rain_days":   None,
        }

    n = len(valid_years)
    return {
        "avg_thunderstorm_days": round(sum(d["thunder"] for d in valid_years) / n, 2),
        "avg_heavy_snow_days":   round(sum(d["snow"]    for d in valid_years) / n, 2),
        "avg_high_wind_days":    round(sum(d["wind"]    for d in valid_years) / n, 2),
        "avg_heavy_rain_days":   round(sum(d["rain"]    for d in valid_years) / n, 2),
    }


# ── Normalisation ─────────────────────────────────────────────────────────────
def minmax_normalize(values: List[Optional[float]]) -> List[Optional[float]]:
    non_none = [v for v in values if v is not None]
    if not non_none:
        return values  # all None
    lo, hi = min(non_none), max(non_none)
    if lo == hi:
        return [0.5 if v is not None else None for v in values]
    return [None if v is None else (v - lo) / (hi - lo) for v in values]


# ── Score computation ─────────────────────────────────────────────────────────
def compute_scores(sites: List[dict]) -> List[dict]:
    metrics = ["avg_thunderstorm_days", "avg_heavy_snow_days", "avg_high_wind_days", "avg_heavy_rain_days"]
    metric_keys = {
        "avg_thunderstorm_days": "thunderstorm",
        "avg_heavy_snow_days":   "snow",
        "avg_high_wind_days":    "wind",
        "avg_heavy_rain_days":   "rain",
    }

    # Gather raw values per metric (None if any metric missing)
    raw: Dict[str, List[Optional[float]]] = {m: [] for m in metrics}
    for s in sites:
        for m in metrics:
            raw[m].append(s.get(m))

    # Normalise each metric
    norm: Dict[str, List[Optional[float]]] = {m: minmax_normalize(raw[m]) for m in metrics}

    for i, s in enumerate(sites):
        norm_vals = {m: norm[m][i] for m in metrics}
        if any(v is None for v in norm_vals.values()):
            s["composite_weather_risk"] = None
        else:
            score = sum(
                WEIGHTS[metric_keys[m]] * norm_vals[m]
                for m in metrics
            )
            s["composite_weather_risk"] = round(score, 4)

    return sites


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    print(f"Reading sites from {EXCEL_PATH} …")
    sites = read_sites(EXCEL_PATH)
    total = len(sites)
    print(f"Found {total} sites to process.\n")

    for i, site in enumerate(sites, 1):
        print(f"Fetching site {i} of {total}: {site['site_id']}", flush=True)
        response = fetch_site_weather(site["site_id"], site["lat"], site["lon"])

        if response is None or "daily" not in response:
            if response is not None:
                logger.error("Site %s: 'daily' key absent in response", site["site_id"])
            site.update({
                "avg_thunderstorm_days": None,
                "avg_heavy_snow_days":   None,
                "avg_high_wind_days":    None,
                "avg_heavy_rain_days":   None,
            })
        else:
            metrics = aggregate_metrics(response["daily"])
            site.update(metrics)

        time.sleep(REQUEST_DELAY)

    print("\nComputing composite risk scores …")
    sites = compute_scores(sites)

    # Output JSON
    output = [
        {
            "site_id":               s["site_id"],
            "region":                s["region"],
            "lat":                   s["lat"],
            "lon":                   s["lon"],
            "tenants":               s["tenants"],
            "avg_thunderstorm_days": s.get("avg_thunderstorm_days"),
            "avg_heavy_snow_days":   s.get("avg_heavy_snow_days"),
            "avg_high_wind_days":    s.get("avg_high_wind_days"),
            "avg_heavy_rain_days":   s.get("avg_heavy_rain_days"),
            "composite_weather_risk": s.get("composite_weather_risk"),
        }
        for s in sites
    ]

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\nWrote {len(output)} entries to {OUTPUT_PATH}")

    # Summary
    scores = [s["composite_weather_risk"] for s in output if s["composite_weather_risk"] is not None]
    nulls  = sum(1 for s in output if s["composite_weather_risk"] is None)
    if scores:
        print(f"\nComposite weather risk summary:")
        print(f"  Min:    {min(scores):.4f}")
        print(f"  Max:    {max(scores):.4f}")
        print(f"  Mean:   {sum(scores)/len(scores):.4f}")
    print(f"  Nulls:  {nulls} / {len(output)}")
    if nulls == len(output):
        logger.warning("All sites failed — all scores are null.")


if __name__ == "__main__":
    main()
