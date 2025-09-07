"""MHVillage API fetcher.

This module exposes a function to fetch MHVillage park details for a county/state
without saving to disk. It can also be run as a script for a quick count.
"""

from __future__ import annotations

import asyncio
import os
import sys
from typing import Any, List, Dict
from urllib.parse import quote_plus

from curl_cffi import requests
from curl_cffi.requests import AsyncSession

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from lib.json_utils import normalize_keys  # noqa: E402


def _headers() -> dict:
    return {
        "sec-ch-ua-platform": '"Windows"',
        "Referer": "https://www.mhvillage.com/parks/ca/riverside-county",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 "
            "Safari/537.36 Edg/139.0.0.0"
        ),
        "Accept": "application/json, text/plain, */*",
        "sec-ch-ua": '"Not;A=Brand";v="99", "Microsoft Edge";v="139", "Chromium";v="139"',
        "Content-Type": "application/vnd.milli+json",
        "sec-ch-ua-mobile": "?0",
    }


def _search_url(county: str, state: str, offset: int, limit: int) -> str:
    return (
        "https://www.mhvillage.com/api/v1/park-searches.json"
    f"?county={quote_plus(county)}&state={state}"
        f"&offset={offset}&limit={limit}&order%5B%5D=best-match:asc&radius=0"
        "&include%5B%5D=photos&include%5B%5D=address&include%5B%5D=homes-count&include%5B%5D=state-association"
    )


DETAIL_INCLUDES = (
    "order%5B%5D=best-match:asc&include%5B%5D=appointment-availability&include%5B%5D=photos&"
    "include%5B%5D=address&include%5B%5D=logo&include%5B%5D=brochure&include%5B%5D=homes-count&"
    "include%5B%5D=site-count&include%5B%5D=details&include%5B%5D=phone&include%5B%5D=alternate-phone&"
    "include%5B%5D=phone&include%5B%5D=state-association&include%5B%5D=favorite-count&include%5B%5D=lead-delivery-methods"
)


def fetch_mhvillage_details(
    county: str = "Riverside",
    state: str = "CA",
    limit: int = 50,
    max_pages: int | None = None,
) -> List[Dict[str, Any]]:
    """Fetch MHVillage park details for a county/state and return a list of dicts.

    - Paginates search endpoint to get park keys.
    - Fetches park detail for each key concurrently.
    - Returns camelCased keys via normalize_keys.
    """
    headers = _headers()

    # 1) Enumerate parks via search
    offset = 0
    all_parks: list[dict] = []
    pages = 0
    while True:
        url = _search_url(county, state, offset, limit)
        resp = requests.get(url, headers=headers)
        try:
            search = resp.json()
        except Exception:
            break

        parks = search.get("payload") or []
        if not isinstance(parks, list) or not parks:
            break
        all_parks.extend(parks)

        total = int(search.get("total") or 0)
        offset += limit
        pages += 1
        if (total and offset >= total) or (max_pages and pages >= max_pages) or offset > 5000:
            break

    park_keys = [p.get("key") for p in all_parks if isinstance(p, dict) and p.get("key")]

    # 2) Fetch details concurrently
    async def _fetch_all(keys: list[int]) -> list[dict]:
        sem = asyncio.Semaphore(10)
        async with AsyncSession() as session:
            async def fetch_one(k: int):
                durl = f"https://www.mhvillage.com/api/v1/parks/{k}.json?{DETAIL_INCLUDES}"
                try:
                    async with sem:
                        r = await session.get(durl, headers=headers)
                    try:
                        return r.json()
                    except Exception:
                        return {"error": True, "key": k, "raw": r.text}
                except Exception as e:
                    return {"error": True, "key": k, "message": str(e)}

            tasks = [fetch_one(k) for k in keys]
            return await asyncio.gather(*tasks)

    details = asyncio.run(_fetch_all(park_keys)) if park_keys else []
    return normalize_keys(details)


if __name__ == "__main__":
    data = fetch_mhvillage_details()
    print(f"Fetched MHVillage details: {len(data)} records")
