"""
inject_site_metadata.py
Enriches the topSites JSON embedded in index.html with power-meter and
tenant data from the TOP 100 Excel file.

Run: python3 inject_site_metadata.py
"""

import json
import re
from pathlib import Path

import pandas as pd

EXCEL_PATH = Path("data/TOP 100 OTA Sites ARN 20260331.xlsx")
HTML_PATH  = Path("index.html")


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
    meta = {}
    for _, row in df.iterrows():
        tenants        = None if pd.isna(row["tenants"])        else int(row["tenants"])
        pm_dist        = None if pd.isna(row["pm_dist"])        else float(row["pm_dist"])
        consumption_kw = None if pd.isna(row["consumption_kw"]) else float(row["consumption_kw"])
        meta[row["site_id"]] = {
            "tenants":       tenants,
            "pmDist":        pm_dist,
            "pmStatus":      pm_status(row["pm_note"]),
            "consumptionKw": consumption_kw,
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
            s.setdefault("tenants",       None)
            s.setdefault("pmDist",        None)
            s.setdefault("pmStatus",      "unknown")
            s.setdefault("consumptionKw", None)

    new_json = json.dumps(sites, ensure_ascii=False, separators=(",", ":"))
    new_html = html[:m.start(2)] + new_json + html[m.end(2):]

    HTML_PATH.write_text(new_html, encoding="utf-8")
    print(f"Done — enriched {enriched}/{len(sites)} sites in {HTML_PATH}")


if __name__ == "__main__":
    main()
