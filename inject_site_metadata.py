"""
inject_site_metadata.py
Enriches the topSites JSON embedded in index.html with power-meter,
tenant, land-use, noise-safety, and DSO data from external data files.

Run: python3 inject_site_metadata.py
"""

import csv
import json
import re
from pathlib import Path

import pandas as pd

EXCEL_PATH     = Path("data/TOP 100 OTA Sites ARN 20260331.xlsx")
LAND_USE_PATH  = Path("site_land_use.json")
DSO_CSV_PATH   = Path("dso_per_site.csv")
HTML_PATH      = Path("index.html")


def pm_status(note: str) -> str:
    """Normalise the Notes field to one of: 'at_site', 'not_at_site', 'needs_check', 'unknown'."""
    if not isinstance(note, str):
        return "unknown"
    n = note.strip().lower()
    if "at site" in n and "not" not in n:
        return "at_site"
    if "not at site" in n:
        return "not_at_site"
    if "needs check" in n:
        return "needs_check"
    return "unknown"


def main() -> None:
    # ── Load Excel ────────────────────────────────────────────
    df = pd.read_excel(EXCEL_PATH, engine="openpyxl")
    col_lower = {c.strip().lower(): c for c in df.columns}

    def col(name: str):
        return col_lower[name.lower()]

    df = df.rename(columns={
        col("site id"):                        "site_id",
        col("current no. of tenants"):         "tenants",
        col("distance from power meter"):      "pm_dist",
        col("notes / additional information"): "pm_note",
        col("consumption of the site (kw)"):   "consumption_kw",
    })
    df["site_id"] = df["site_id"].astype(str)
    # ── Load land-use / noise-safety classifications ──────────
    land_use_map = {}
    noise_safe_map = {}
    nearby_landuse_map = {}
    residential_count_map = {}
    if LAND_USE_PATH.exists():
        for entry in json.loads(LAND_USE_PATH.read_text(encoding="utf-8")):
            sid = str(entry["site_id"])
            land_use_map[sid] = entry.get("land_use", "unknown")
            noise_safe_map[sid] = entry.get("noise_safe")
            nearby_landuse_map[sid] = entry.get("nearby_landuse", "unknown")
            residential_count_map[sid] = entry.get("residential_count", None)
    else:
        print(f"Warning: {LAND_USE_PATH} not found — landUse will default to 'unknown'")

    # ── Load DSO mapping ────────────────────────────────────
    dso_map = {}
    if DSO_CSV_PATH.exists():
        with open(DSO_CSV_PATH, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                dso_map[row["site_id"]] = row.get("dso_name", "")
    else:
        print(f"Warning: {DSO_CSV_PATH} not found — dso will default to empty")

    meta = {}
    for _, row in df.iterrows():
        tenants        = None if pd.isna(row["tenants"])        else int(row["tenants"])
        pm_dist        = None if pd.isna(row["pm_dist"])        else float(row["pm_dist"])
        consumption_kw = None if pd.isna(row["consumption_kw"]) else float(row["consumption_kw"])
        sid = row["site_id"]
        meta[sid] = {
            "tenants":          tenants,
            "pmDist":           pm_dist,
            "pmStatus":         pm_status(row["pm_note"]),
            "consumptionKw":    consumption_kw,
            "landUse":          land_use_map.get(sid, "unknown"),
            "noiseSafe":        noise_safe_map.get(sid),
            "nearbyLanduse":    nearby_landuse_map.get(sid, "unknown"),
            "residentialCount": residential_count_map.get(sid),
            "dso":              dso_map.get(sid, ""),
        }

    # ── Read HTML ─────────────────────────────────────────────
    html = HTML_PATH.read_text(encoding="utf-8")

    # Extract the existing topSites JSON array
    pattern = r'(const topSites = )(\[.*?\]);'
    m = re.search(pattern, html, re.DOTALL)
    if not m:
        raise RuntimeError("Could not find 'const topSites = [...]' in index.html")

    sites = json.loads(m.group(2))
    enriched = 0
    for s in sites:
        sid = s.get("id", "")
        if sid in meta:
            s.update(meta[sid])
            enriched += 1
        else:
            # defaults so JS never sees undefined
            s.setdefault("tenants",          None)
            s.setdefault("pmDist",           None)
            s.setdefault("pmStatus",         "unknown")
            s.setdefault("consumptionKw",    None)
            s.setdefault("landUse",          "unknown")
            s.setdefault("noiseSafe",        None)
            s.setdefault("nearbyLanduse",    "unknown")
            s.setdefault("residentialCount", None)
            s.setdefault("dso",              "")

    new_json = json.dumps(sites, ensure_ascii=False, separators=(",", ":"))
    new_html = html[:m.start(2)] + new_json + html[m.end(2):]

    HTML_PATH.write_text(new_html, encoding="utf-8")
    print(f"Done — enriched {enriched}/{len(sites)} sites in {HTML_PATH}")


if __name__ == "__main__":
    main()
