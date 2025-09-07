#!/usr/bin/env python3
"""
Simple CLI wrapper around the scrapers.

Examples:
  python scrape.py --source ca_hcd --county "Riverside" --limit 200 --out data/parks.json
  python scrape.py --source mhvillage --county "Riverside" --state CA --limit 200 --out data/parks.csv
  python scrape.py --source rivcoview --county "Riverside" --limit 200 --out data/rivco.json

Notes:
- For CA HCD, county is mapped to the HCD county code. Riverside => 33.
- If your county name isn't recognized for HCD, pass --county-code explicitly.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, List

import pandas as pd


# Minimal county name -> HCD county code mapping. Extend as needed.
HCD_COUNTY_CODE = {
    # Commonly used
    "riverside": "33",
}


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def to_dataframe(data: Any) -> pd.DataFrame:
    if data is None:
        return pd.DataFrame()
    if isinstance(data, list):
        try:
            return pd.json_normalize(data)
        except Exception:
            return pd.DataFrame(data)
    if isinstance(data, dict):
        return pd.json_normalize([data])
    return pd.DataFrame()


def write_output(data: List[dict] | Any, out_path: Path) -> None:
    ext = out_path.suffix.lower()
    ensure_parent(out_path)
    if ext == ".json":
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return
    if ext == ".csv":
        df = to_dataframe(data)
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        return
    raise SystemExit(f"Unsupported output extension: {ext}. Use .json or .csv")


def run_cli() -> None:
    parser = argparse.ArgumentParser(description="Scrape data from configured sources and save to JSON/CSV.")
    parser.add_argument(
        "--source",
        required=True,
        choices=["ca_hcd", "mhvillage", "rivcoview"],
        help="Which scraper to run",
    )
    parser.add_argument(
        "--county",
        default="Riverside",
        help="County name (used by mhvillage and rivcoview; mapped to code for ca_hcd)",
    )
    parser.add_argument(
        "--state",
        default="CA",
        help="State code for mhvillage (default: CA)",
    )
    parser.add_argument(
        "--county-code",
        dest="county_code",
        default=None,
        help="Override HCD county code (e.g., 33 for Riverside). If provided, overrides --county for ca_hcd",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Max number of records to write (applied after fetch)",
    )
    parser.add_argument(
        "--out",
        required=True,
        type=Path,
        help="Output file path (.json or .csv)",
    )

    args = parser.parse_args()

    source = args.source
    county = args.county
    state = args.state
    county_code = args.county_code
    limit = max(0, int(args.limit)) if args.limit is not None else 0
    out_path: Path = args.out

    data: List[dict] | Any = []

    if source == "ca_hcd":
        # Lazy import to allow --help without deps
        from scraper.ca_hcd import fetch_ca_hcd

        code = county_code
        if not code:
            key = (county or "").strip().lower()
            code = HCD_COUNTY_CODE.get(key)
            if not code:
                print(
                    f"Warning: Unrecognized county '{county}' for CA HCD; defaulting to Riverside (33). "
                    "Use --county-code to specify explicitly.",
                    file=sys.stderr,
                )
                code = HCD_COUNTY_CODE["riverside"]
        data = fetch_ca_hcd(code)

    elif source == "mhvillage":
        from scraper.mhvillage import fetch_mhvillage_details

        # Use default pagination inside, then slice
        data = fetch_mhvillage_details(county=county, state=state)

    elif source == "rivcoview":
        from scraper.rivcoview import fetch_rivcoview

        data = fetch_rivcoview(query_value=county, limit_rows=limit)

    # Enforce limit after fetch if requested
    if isinstance(data, list) and limit:
        data = data[:limit]

    # Persist
    write_output(data, out_path)

    size = len(data) if isinstance(data, list) else (1 if data else 0)
    print(f"Wrote {size} records to {out_path}")


if __name__ == "__main__":
    run_cli()
