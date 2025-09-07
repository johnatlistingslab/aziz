"""RivCoView API fetcher.

Expose a function to fetch APN details for parcels with street_address including
the provided query (default 'Riverside'). Returns normalized data without saving.
"""

from __future__ import annotations

import asyncio
import os
import sys
from typing import Any, List

from curl_cffi import requests
from curl_cffi.requests import AsyncSession

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from lib.json_utils import normalize_keys  # noqa: E402


BASE_URL = "https://rivcoview.rivcoacr.org/data/ajaxcalls/db/getData.php"


def _headers() -> dict:
    return {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': 'https://rivcoview.rivcoacr.org',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://rivcoview.rivcoacr.org/?utm_source=chatgpt.com',
        'sec-ch-ua': '"Not;A=Brand";v="99", "Microsoft Edge";v="139", "Chromium";v="139"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36 Edg/139.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
        'Cookie': 'surveym_link=1'
    }


def fetch_rivcoview(query_value: str = "Riverside", limit_rows: int | None = 200) -> List[Any]:
    """Fetch RivCoView detailed records for parcels matching street_address value.

    Returns a list of normalized detail records (list or dict items as returned by API).
    """
    search_payload = f"qtype=assessment_info&field=mv_Location%3Astreet_address&value={query_value}"
    resp = requests.post(BASE_URL, headers=_headers(), data=search_payload)
    try:
        search_data = resp.json()
    except Exception:
        import json as _json
        search_data = _json.loads(resp.text)

    rows = search_data.get("rows") if isinstance(search_data, dict) else None
    if not rows or not isinstance(rows, list):
        return []

    apns = [r.get("apn") for r in rows if isinstance(r, dict) and r.get("apn")]
    if not apns:
        return []

    city_by_apn = {r.get("apn"): r.get("situs_city") for r in rows if isinstance(r, dict) and r.get("apn")}

    detail_headers = _headers()

    async def fetch_all_details(pin_values: list[str]) -> list:
        sem = asyncio.Semaphore(10)
        async with AsyncSession() as session:
            async def fetch_one(pin: str):
                pin_payload = f"qtype=assessment_info&field=mv_Location%3APIN&value={pin}"
                try:
                    async with sem:
                        r = await session.post(BASE_URL, headers=detail_headers, data=pin_payload)
                    try:
                        return r.json()
                    except Exception:
                        return {"error": True, "pin": pin, "raw": r.text}
                except Exception as e:
                    return {"error": True, "pin": pin, "message": str(e)}

            tasks = [fetch_one(p) for p in pin_values]
            return await asyncio.gather(*tasks)

    details = asyncio.run(fetch_all_details(apns))

    # Enrich with city
    for idx, apn in enumerate(apns):
        try:
            rec = details[idx]
            city = city_by_apn.get(apn)
            if not city:
                continue
            if isinstance(rec, dict):
                rec.setdefault("city", city)
            elif isinstance(rec, list):
                for item in rec:
                    if isinstance(item, dict):
                        item.setdefault("city", city)
        except Exception:
            pass

    return normalize_keys(details)