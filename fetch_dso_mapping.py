#!/usr/bin/env python3
"""
Fetch DSO (Distribution System Operator) mapping for Austrian telco sites
using E-Control's Tarifkalkulator API.

API endpoint discovered from E-Control's rc-public-portlet:
  GET /o/rc-public-rest/rate-calculator/grid-operators?zipCode=<PLZ>&energyType=POWER

Usage: python fetch_dso_mapping.py
"""

import csv
import re
import time
from collections import defaultdict

import openpyxl
import requests

EXCEL_FILE = "data/TOP 100 OTA Sites ARN 20260327.xlsx"
API_URL = "https://www.e-control.at/o/rc-public-rest/rate-calculator/grid-operators"
OUTPUT_SITE_CSV = "dso_per_site.csv"
OUTPUT_SUMMARY_CSV = "dso_summary.csv"
REQUEST_DELAY = 1  # seconds between requests


def load_sites(path):
    """Load site data from Excel file."""
    wb = openpyxl.load_workbook(path, read_only=True)
    ws = wb["Tabelle1"]
    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    sites = []
    for row in rows[1:]:  # skip header
        if row[0] is None:
            continue
        postcode = str(row[5]).strip()
        # Clean postcodes like "A-9020" -> "9020"
        postcode = re.sub(r"^[A-Za-z]-", "", postcode)
        sites.append({
            "site_id": str(row[1]),
            "postcode": postcode,
            "city": str(row[4]),
            "region": str(row[3]),
        })
    return sites


def lookup_dso(postcode, session):
    """Query E-Control API for DSOs serving a given postcode."""
    resp = session.get(
        API_URL,
        params={"zipCode": postcode, "energyType": "POWER"},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()

    if not data.get("isZipCodeValid"):
        return []

    results = []
    for op in data.get("gridOperators", []):
        results.append({
            "name": op.get("name", ""),
            "id": op.get("id"),
        })
    return results


def main():
    sites = load_sites(EXCEL_FILE)
    print(f"Loaded {len(sites)} sites from {EXCEL_FILE}")

    # Deduplicate postcodes to avoid redundant API calls
    unique_postcodes = sorted(set(s["postcode"] for s in sites))
    print(f"Found {len(unique_postcodes)} unique postcodes to look up\n")

    # Look up DSOs per postcode
    dso_by_postcode = {}
    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; DSO-Lookup/1.0; one-time data enrichment)",
    })

    for i, plz in enumerate(unique_postcodes, 1):
        city = next((s["city"] for s in sites if s["postcode"] == plz), "")
        print(f"Looking up postcode {i} of {len(unique_postcodes)}: {plz} - {city}")

        try:
            operators = lookup_dso(plz, session)
            if operators:
                dso_by_postcode[plz] = operators
            else:
                print(f"  WARNING: No DSOs found for postcode {plz}")
                dso_by_postcode[plz] = [{"name": "no DSO found", "id": None}]
        except Exception as e:
            print(f"  ERROR looking up {plz}: {e}")
            dso_by_postcode[plz] = [{"name": "lookup failed", "id": None}]

        if i < len(unique_postcodes):
            time.sleep(REQUEST_DELAY)

    # Write dso_per_site.csv
    dso_counts = defaultdict(list)  # dso_name -> [site_ids]

    with open(OUTPUT_SITE_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["site_id", "postcode", "city", "region", "dso_name", "dso_website"])

        for site in sites:
            operators = dso_by_postcode.get(site["postcode"], [])
            dso_names = ", ".join(op["name"] for op in operators)
            # No website info available from this API endpoint
            dso_website = ""

            writer.writerow([
                site["site_id"],
                site["postcode"],
                site["city"],
                site["region"],
                dso_names,
                dso_website,
            ])

            # Track DSO counts (count each DSO separately when multiple per postcode)
            for op in operators:
                dso_counts[op["name"]].append(site["site_id"])

    print(f"\nWrote {OUTPUT_SITE_CSV}")

    # Write dso_summary.csv
    summary = sorted(dso_counts.items(), key=lambda x: len(x[1]), reverse=True)

    with open(OUTPUT_SUMMARY_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["dso_name", "site_count", "sites_list"])

        for dso_name, site_ids in summary:
            writer.writerow([dso_name, len(site_ids), ", ".join(site_ids)])

    print(f"Wrote {OUTPUT_SUMMARY_CSV}")

    # Final summary
    valid_dsos = [name for name, _ in summary if name not in ("lookup failed", "no DSO found")]
    print(f"\nTotal unique DSOs found: {len(valid_dsos)}")
    print("\nDSO distribution:")
    for dso_name, site_ids in summary:
        print(f"  {dso_name}: {len(site_ids)} sites")


if __name__ == "__main__":
    main()
