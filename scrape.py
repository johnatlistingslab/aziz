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
import time
from datetime import datetime
from pathlib import Path
from typing import Any, List
import threading

import pandas as pd


# Minimal county name -> HCD county code mapping. Extend as needed.
HCD_COUNTY_CODE = {
    # Commonly used
    "riverside": "33",
}


class _Spinner:
    """Lightweight CLI spinner for long-running fetches.

    Prints to stderr to avoid mixing with JSON/CSV stdout use cases.
    Automatically no-ops when not a TTY.
    """

    FRAMES = ["|", "/", "-", "\\"]

    def __init__(self, message: str = "Working...") -> None:
        self.message = message
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._isatty = sys.stderr.isatty() if hasattr(sys.stderr, "isatty") else True

    def start(self) -> None:
        if not self._isatty:
            # Fall back to a single status line
            print(f"⏳ {self.message}", file=sys.stderr, flush=True)
            return

        def run():
            i = 0
            while not self._stop.is_set():
                frame = self.FRAMES[i % len(self.FRAMES)]
                sys.stderr.write(f"\r⏳ {self.message} {frame}")
                sys.stderr.flush()
                i += 1
                self._stop.wait(0.1)
            # Clear spinner line on stop
            sys.stderr.write("\r" + " " * (len(self.message) + 6) + "\r")
            sys.stderr.flush()

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()

    def stop(self, final_message: str | None = None) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1)
        if final_message:
            print(final_message, file=sys.stderr, flush=True)


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
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print extra debug info during the run",
    )

    args = parser.parse_args()

    source = args.source
    county = args.county
    state = args.state
    county_code = args.county_code
    limit = max(0, int(args.limit)) if args.limit is not None else 0
    out_path: Path = args.out
    verbose: bool = bool(args.verbose)

    data_raw: List[dict] | Any = []

    # Timing
    t0 = time.perf_counter()
    start_iso = datetime.now().isoformat(timespec="seconds")

    # Live progress indicator
    spin_msg = {
        "ca_hcd": "Fetching CA HCD parks",
        "mhvillage": "Fetching MHVillage community details (can take a while)",
        "rivcoview": "Fetching RivCoView parcel details",
    }.get(source, "Fetching data")
    spinner = _Spinner(spin_msg)

    try:
        spinner.start()

        if source == "ca_hcd":
            # Lazy import to allow --help without deps
            from scraper.ca_hcd import fetch_ca_hcd

            code = county_code
            if not code:
                key = (county or "").strip().lower()
                code = HCD_COUNTY_CODE.get(key)
                if not code:
                    print(
                        (
                            f"Warning: Unrecognized county '{county}' for CA HCD; defaulting to Riverside (33). "
                            "Use --county-code to specify explicitly."
                        ),
                        file=sys.stderr,
                    )
                    code = HCD_COUNTY_CODE["riverside"]
            data_raw = fetch_ca_hcd(code)

        elif source == "mhvillage":
            from scraper.mhvillage import fetch_mhvillage_details

            # Use default pagination inside, then slice
            data_raw = fetch_mhvillage_details(county=county, state=state)

        elif source == "rivcoview":
            from scraper.rivcoview import fetch_rivcoview

            data_raw = fetch_rivcoview(query_value=county, limit_rows=limit)

    except KeyboardInterrupt:
        spinner.stop("⚠️  Aborted by user.")
        elapsed = time.perf_counter() - t0
        print(
            (
                "Run aborted\n"
                f"  Source: {source}\n"
                f"  Duration: {elapsed:.2f}s\n"
                f"  Started: {start_iso}\n"
            ),
            file=sys.stdout,
        )
        raise SystemExit(130)
    except Exception as e:
        spinner.stop("❌ Fetch failed.")
        elapsed = time.perf_counter() - t0
        print(
            (
                "Run failed\n"
                f"  Source: {source}\n"
                f"  Error: {e}\n"
                f"  Duration: {elapsed:.2f}s\n"
                f"  Started: {start_iso}\n"
            ),
            file=sys.stdout,
        )
        raise SystemExit(1)
    else:
        spinner.stop("✅ Fetch complete.")

    # Enforce limit after fetch if requested
    size_raw = len(data_raw) if isinstance(data_raw, list) else (1 if data_raw else 0)
    data: List[dict] | Any = data_raw
    if isinstance(data_raw, list) and limit:
        data = data_raw[:limit]

    # Persist
    write_output(data, out_path)

    # Compute run stats
    end_iso = datetime.now().isoformat(timespec="seconds")
    elapsed = time.perf_counter() - t0
    final_count = len(data) if isinstance(data, list) else (1 if data else 0)
    rate = (final_count / elapsed) if elapsed > 0 else float("inf")
    try:
        bytes_out = out_path.stat().st_size
    except Exception:
        bytes_out = 0

    # Detailed run summary
    lines = [
        "Run summary",
        f"  Source: {source}",
    ]
    if source == "ca_hcd":
        lines.append(f"  County code: {code}")
    if source == "mhvillage":
        lines.append(f"  County/State: {county}, {state}")
    if source == "rivcoview":
        lines.append(f"  Query value: {county}")
    lines.extend(
        [
            f"  Limit requested: {limit}",
            f"  Records fetched (raw): {size_raw}",
            f"  Records written: {final_count}",
            f"  Output file: {out_path}",
            f"  Output size: {bytes_out:,} bytes",
            f"  Started: {start_iso}",
            f"  Finished: {end_iso}",
            f"  Duration: {elapsed:.2f}s",
            f"  Throughput: {rate:.2f} rec/s",
        ]
    )
    if verbose:
        lines.append("  Mode: verbose")

    print("\n".join(lines))


if __name__ == "__main__":
    run_cli()
