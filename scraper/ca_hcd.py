"""CA HCD API fetcher.

Expose a function to fetch Mobile Home Park search results for a given county
without saving to disk. Default county code 33 = Riverside.
"""

from __future__ import annotations

import os
import sys
from typing import Any

from curl_cffi import requests

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from lib.json_utils import normalize_keys  # noqa: E402


def _headers() -> dict:
    return {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "origin": "https://cahcd.my.site.com",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "referer": "https://cahcd.my.site.com/s/mobilehomeparksearch",
        "sec-ch-ua": '"Not;A=Brand";v="99", "Microsoft Edge";v="139", "Chromium";v="139"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36 Edg/139.0.0.0"
        ),
    }


def fetch_ca_hcd(county_code: str = "33") -> Any:
    """Fetch CA HCD Mobile Home Park search results for a county code.

    Returns the parsed JSON (normalized keys) without saving to disk.
    county_code "33" corresponds to Riverside County.
    """
    url = "https://cahcd.my.site.com/s/sfsites/aura?r=4&aura.ApexAction.execute=1"

    payload = (
        "message=%7B%22actions%22%3A%5B%7B%22id%22%3A%22148%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2F"
        "ApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22"
        "%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22MobileHomeParksSearchController%22%2C%22"
        "method%22%3A%22getSearchResults%22%2C%22params%22%3A%7B%22searchParams%22%3A%22%7B%5C%22parkstatus%5C%22%3A%5C%22All%5C%22%2C%5C%22county%5C%22%3A%5C%22"
        f"{county_code}"
        "%5C%22%2C%5C%22city%5C%22%3A%5C%22All%20Cities%5C%22%7D%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&"
        "aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22eE5UbjZPdVlRT3M0d0xtOXc5MzVOQWg5TGxiTHU3MEQ5RnBMM0VzVXc1cmcxMi42MjkxNDU2LjE2Nzc3MjE2%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%221305_7pTC6grCTP7M16KdvDQ-Xw%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Atrue%7D&"
        "aura.pageURI=%2Fs%2Fmobilehomeparksearch&aura.token=null"
    )

    resp = requests.post(url, headers=_headers(), data=payload)
    try:
        data = resp.json()
    except Exception:
        return None

    # Salesforce Aura responses are nested; extract queryResults if present
    actions = data.get("actions", []) if isinstance(data, dict) else []
    query_results = None
    if actions:
        try:
            for act in actions:
                rv = act.get("returnValue") if isinstance(act, dict) else None
                if isinstance(rv, dict):
                    # sometimes nested under 'returnValue' again
                    inner = rv.get("returnValue") if isinstance(rv.get("returnValue"), dict) else rv
                    if isinstance(inner, dict) and "queryResults" in inner:
                        query_results = inner.get("queryResults")
                        break
        except Exception:
            query_results = None

    payload_to_return = query_results if query_results is not None else data
    return normalize_keys(payload_to_return)


if __name__ == "__main__":
    out = fetch_ca_hcd()
    size = len(out) if isinstance(out, list) else (1 if out else 0)
    print(f"Fetched CA HCD: {size} rows")
"""
{
    "actions": [
        {
            "id": "148;a",
            "state": "SUCCESS",
            "returnValue": {
                "returnValue": {
                    "queryResults": [
                        {
                            "ADDRESS__c": "39556 PETERSON RD",
                            "CITY__c": "RANCHO MIRAGE",
                            "COUNTY_NAME__c": "RIVERSIDE",
                            "COUNTY_NUMBER__c": "33",
                            "NUMBER_RV_LOTS_NO_DRAINS__c": 0,
                            "PARK_IDENTIFIER__c": "33-0478-MP",
                            "PARK_NAME__c": "BLUE HEAVEN MHP",
                            "PARK_OPERATOR_ADDRESS__c": "69-825 HWY 111",
                            "PARK_OPERATOR_CITY__c": "RANCHO MIRAGE",
                            "PARK_OPERATOR_NAME__c": "HOUSING AUTHORITY CITY OF RANCHO MIRAGE",
                            "PARK_OPERATOR_STATE__c": "CA",
                            "PARK_OPERATOR_ZIP_CODE__c": "92270",
                            "JURISDICTION_ADDRESS__c": "MOBILEHOME PARKS PROGRAM 69-825 HWY 111",
                            "JURISDICTION_CITY__c": "RANCHO MIRAGE",
                            "JURISDICTION_PHONE__c": "7602029253",
                            "JURISDICTION_STATE__c": "CA",
                            "JURISDICTION_ZIP_CODE__c": "92270",
                            "PHONE_NUMBER__c": "6193282600",
                            "STATUS_ID__c": 2449,
                            "TOTAL_NUMBER_LOTS__c": 16,
                            "ZIP_CODE__c": "92270",
                            "JURISDICTION_NAME__c": "RANCHO MIRAGE",
                            "JURISDICTION_CONTACT_NAME__c": "MURCHALL- CBO, JACK",
                            "NUMBER_RV_LOTS_DRAINS__c": 2,
                            "NUMBER_MH_LOTS__c": 14,
                            "Id": "x0ucs00000EREUgAAP"
                        },
                        {
                            "ADDRESS__c": "70 260 HWY 111",
                            "CITY__c": "RANCHO MIRAGE",
                            "COUNTY_NAME__c": "RIVERSIDE",
                            "COUNTY_NUMBER__c": "33",
                            "FIRE_ENFORCEMENT_NAME__c": "HCD - SOUTHERN AREA OFFICE",
                            "NUMBER_RV_LOTS_NO_DRAINS__c": 0,
                            "PARK_IDENTIFIER__c": "33-0477-MP",
                            "PARK_NAME__c": "BLUE SKIES VILLAGE",
                            "PARK_OPERATOR_ADDRESS__c": "70-260 HWY 111",
                            "PARK_OPERATOR_CITY__c": "RANCHO MIRAGE",
                            "PARK_OPERATOR_NAME__c": "BLUE SKIES VILLAGE HOA",
                            "PARK_OPERATOR_STATE__c": "CA",
                            "PARK_OPERATOR_ZIP_CODE__c": "92270",
                            "JURISDICTION_ADDRESS__c": "MOBILEHOME PARKS PROGRAM 69-825 HWY 111",
                            "JURISDICTION_CITY__c": "RANCHO MIRAGE",
                            "JURISDICTION_PHONE__c": "7602029253",
                            "JURISDICTION_STATE__c": "CA",
                            "JURISDICTION_ZIP_CODE__c": "92270",
                            "PHONE_NUMBER__c": "7603282600",
                            "FIRE_ENFORCEMENT_ADDRESS__c": "3737 MAIN ST 400",
                            "FIRE_ENFORCEMENT_CITY__c": "RIVERSIDE",
                            "FIRE_ENFORCEMENT_PHONE__c": "18009528356",
                            "FIRE_ENFORCEMENT_STATE__c": "CA",
                            "FIRE_ENFORCEMENT_ZIP_CODE__c": "92501",
                            "STATUS_ID__c": 2448,
                            "TOTAL_NUMBER_LOTS__c": 141,
                            "ZIP_CODE__c": "92270",
                            "JURISDICTION_NAME__c": "RANCHO MIRAGE",
                            "JURISDICTION_CONTACT_NAME__c": "MURCHALL- CBO, JACK",
                            "NUMBER_RV_LOTS_DRAINS__c": 0,
                            "NUMBER_MH_LOTS__c": 141,
                            "Id": "x0ucs00000EREUhAAP"
                        },
                        {
                            "ADDRESS__c": "69975 FRANK SINATRA DR",
                            "CITY__c": "RANCHO MIRAGE",
                            "COUNTY_NAME__c": "RIVERSIDE",
                            "COUNTY_NUMBER__c": "33",
                            "FIRE_ENFORCEMENT_NAME__c": "HCD - SOUTHERN AREA OFFICE",
                            "NUMBER_RV_LOTS_NO_DRAINS__c": 0,
                            "PARK_IDENTIFIER__c": "33-0479-MP",
                            "PARK_NAME__c": "RANCHO MIRAGE MOBILEHOME COMMUNITY",
                            "PARK_OPERATOR_ADDRESS__c": "PO BOX 11427",
                            "PARK_OPERATOR_CITY__c": "SANTA ANA",
                            "PARK_OPERATOR_NAME__c": "RANCHO MIRAGE MOBILEHOME COMMUNITYLP",
                            "PARK_OPERATOR_STATE__c": "CA",
                            "PARK_OPERATOR_ZIP_CODE__c": "92711",
                            "JURISDICTION_ADDRESS__c": "MOBILEHOME PARKS PROGRAM 69-825 HWY 111",
                            "JURISDICTION_CITY__c": "RANCHO MIRAGE",
                            "JURISDICTION_PHONE__c": "7602029253",
                            "JURISDICTION_STATE__c": "CA",
                            "JURISDICTION_ZIP_CODE__c": "92270",
                            "PHONE_NUMBER__c": "7603287664",
                            "FIRE_ENFORCEMENT_ADDRESS__c": "3737 MAIN ST 400",
                            "FIRE_ENFORCEMENT_CITY__c": "RIVERSIDE",
                            "FIRE_ENFORCEMENT_PHONE__c": "18009528356",
                            "FIRE_ENFORCEMENT_STATE__c": "CA",
                            "FIRE_ENFORCEMENT_ZIP_CODE__c": "92501",
                            "STATUS_ID__c": 2448,
                            "TOTAL_NUMBER_LOTS__c": 288,
                            "ZIP_CODE__c": "92270",
                            "JURISDICTION_NAME__c": "RANCHO MIRAGE",
                            "JURISDICTION_CONTACT_NAME__c": "MURCHALL- CBO, JACK",
                            "NUMBER_RV_LOTS_DRAINS__c": 0,
                            "NUMBER_MH_LOTS__c": 288,
                            "Id": "x0ucs00000ERFqkAAH"
                        },
                        {
                            "ADDRESS__c": "70 210 HWY 111",
                            "CITY__c": "RANCHO MIRAGE",
                            "COUNTY_NAME__c": "RIVERSIDE",
                            "COUNTY_NUMBER__c": "33",
                            "FIRE_ENFORCEMENT_NAME__c": "HCD - SOUTHERN AREA OFFICE",
                            "NUMBER_RV_LOTS_NO_DRAINS__c": 0,
                            "PARK_IDENTIFIER__c": "33-0476-MP",
                            "PARK_NAME__c": "RANCHO MIRAGE RV & MOBILE VILLAGE LLC",
                            "PARK_OPERATOR_ADDRESS__c": "PO BOX 1949",
                            "PARK_OPERATOR_CITY__c": "RANCHO MIRAGE",
                            "PARK_OPERATOR_NAME__c": "PACERA, PAUL",
                            "PARK_OPERATOR_STATE__c": "CA",
                            "PARK_OPERATOR_ZIP_CODE__c": "92270",
                            "JURISDICTION_ADDRESS__c": "MOBILEHOME PARKS PROGRAM 69-825 HWY 111",
                            "JURISDICTION_CITY__c": "RANCHO MIRAGE",
                            "JURISDICTION_PHONE__c": "7602029253",
                            "JURISDICTION_STATE__c": "CA",
                            "JURISDICTION_ZIP_CODE__c": "92270",
                            "PHONE_NUMBER__c": "7603281177",
                            "FIRE_ENFORCEMENT_ADDRESS__c": "3737 MAIN ST 400",
                            "FIRE_ENFORCEMENT_CITY__c": "RIVERSIDE",
                            "FIRE_ENFORCEMENT_PHONE__c": "18009528356",
                            "FIRE_ENFORCEMENT_STATE__c": "CA",
                            "FIRE_ENFORCEMENT_ZIP_CODE__c": "92501",
                            "STATUS_ID__c": 2448,
                            "TOTAL_NUMBER_LOTS__c": 72,
                            "ZIP_CODE__c": "92270",
                            "JURISDICTION_NAME__c": "RANCHO MIRAGE",
                            "JURISDICTION_CONTACT_NAME__c": "MURCHALL- CBO, JACK",
                            "NUMBER_RV_LOTS_DRAINS__c": 24,
                            "NUMBER_MH_LOTS__c": 48,
                            "Id": "x0ucs00000ERFfgAAH"
                        },
                        {
                            "ADDRESS__c": "70377 GERALD FORD DR",
                            "CITY__c": "RANCHO MIRAGE",
                            "COUNTY_NAME__c": "RIVERSIDE",
                            "COUNTY_NUMBER__c": "33",
                            "FIRE_ENFORCEMENT_NAME__c": "HCD - SOUTHERN AREA OFFICE",
                            "NUMBER_RV_LOTS_NO_DRAINS__c": 0,
                            "PARK_IDENTIFIER__c": "33-0446-MP",
                            "PARK_NAME__c": "THE COLONY",
                            "PARK_OPERATOR_ADDRESS__c": "70377 GERALD FORD",
                            "PARK_OPERATOR_CITY__c": "RANCHO MIRAGE",
                            "PARK_OPERATOR_NAME__c": "HOMETOWN AMERICA LLC",
                            "PARK_OPERATOR_STATE__c": "CA",
                            "PARK_OPERATOR_ZIP_CODE__c": "92270",
                            "JURISDICTION_ADDRESS__c": "MOBILEHOME PARKS PROGRAM 69-825 HWY 111",
                            "JURISDICTION_CITY__c": "RANCHO MIRAGE",
                            "JURISDICTION_PHONE__c": "7602029253",
                            "JURISDICTION_STATE__c": "CA",
                            "JURISDICTION_ZIP_CODE__c": "92270",
                            "PHONE_NUMBER__c": "7603286000",
                            "FIRE_ENFORCEMENT_ADDRESS__c": "3737 MAIN ST 400",
                            "FIRE_ENFORCEMENT_CITY__c": "RIVERSIDE",
                            "FIRE_ENFORCEMENT_PHONE__c": "18009528356",
                            "FIRE_ENFORCEMENT_STATE__c": "CA",
                            "FIRE_ENFORCEMENT_ZIP_CODE__c": "92501",
                            "STATUS_ID__c": 2448,
                            "TOTAL_NUMBER_LOTS__c": 220,
                            "ZIP_CODE__c": "92270",
                            "JURISDICTION_NAME__c": "RANCHO MIRAGE",
                            "JURISDICTION_CONTACT_NAME__c": "MURCHALL- CBO, JACK",
                            "NUMBER_RV_LOTS_DRAINS__c": 0,
                            "NUMBER_MH_LOTS__c": 220,
                            "Id": "x0ucs00000EREUiAAP"
                        },
                        {
                            "ADDRESS__c": "39360 PETERSON RD",
                            "CITY__c": "RANCHO MIRAGE",
                            "COUNTY_NAME__c": "RIVERSIDE",
                            "COUNTY_NUMBER__c": "33",
                            "FIRE_ENFORCEMENT_NAME__c": "HCD - SOUTHERN AREA OFFICE",
                            "NUMBER_RV_LOTS_NO_DRAINS__c": 0,
                            "PARK_IDENTIFIER__c": "33-0475-MP",
                            "PARK_NAME__c": "RANCHO PALMS MHP",
                            "PARK_OPERATOR_ADDRESS__c": "69825 HIGHWAY 111",
                            "PARK_OPERATOR_CITY__c": "RANCHO MIRAGE",
                            "PARK_OPERATOR_NAME__c": "CITY OF RANCHO MIRAGE, HOUSING AUTHORITY",
                            "PARK_OPERATOR_STATE__c": "CA",
                            "PARK_OPERATOR_ZIP_CODE__c": "92270",
                            "JURISDICTION_ADDRESS__c": "MOBILEHOME PARKS PROGRAM 69-825 HWY 111",
                            "JURISDICTION_CITY__c": "RANCHO MIRAGE",
                            "JURISDICTION_PHONE__c": "7602029253",
                            "JURISDICTION_STATE__c": "CA",
                            "JURISDICTION_ZIP_CODE__c": "92270",
                            "PHONE_NUMBER__c": "7603284323",
                            "FIRE_ENFORCEMENT_ADDRESS__c": "3737 MAIN ST 400",
                            "FIRE_ENFORCEMENT_CITY__c": "RIVERSIDE",
                            "FIRE_ENFORCEMENT_PHONE__c": "18009528356",
                            "FIRE_ENFORCEMENT_STATE__c": "CA",
                            "FIRE_ENFORCEMENT_ZIP_CODE__c": "92501",
                            "STATUS_ID__c": 2449,
                            "TOTAL_NUMBER_LOTS__c": 120,
                            "ZIP_CODE__c": "92270",
                            "JURISDICTION_NAME__c": "RANCHO MIRAGE",
                            "JURISDICTION_CONTACT_NAME__c": "MURCHALL- CBO, JACK",
                            "NUMBER_RV_LOTS_DRAINS__c": 0,
                            "NUMBER_MH_LOTS__c": 120,
                            "Id": "x0ucs00000EREUjAAP"
                        },
                        {
                            "ADDRESS__c": "350 E SAN JACINTO AVE",
                            "CITY__c": "PERRIS",
                            "COUNTY_NAME__c": "RIVERSIDE",
                            "COUNTY_NUMBER__c": "33",
                            "FIRE_ENFORCEMENT_NAME__c": "HCD - SOUTHERN AREA OFFICE",
                            "NUMBER_RV_LOTS_NO_DRAINS__c": 0,
                            "PARK_IDENTIFIER__c": "33-0406-MP",
                            "PARK_NAME__c": "LAKE PERRIS VILLAGE MFD HOME COMMUNITY",
                            "PARK_OPERATOR_ADDRESS__c": "1515 THE ALAMEDA SUITE # 200",
                            "PARK_OPERATOR_CITY__c": "SAN JOSE",
                            "PARK_OPERATOR_NAME__c": "WESTWIND ENTERPRISES LTD",
                            "PARK_OPERATOR_STATE__c": "CA",
                            "PARK_OPERATOR_ZIP_CODE__c": "95126",
                            "JURISDICTION_ADDRESS__c": "3737 MAIN ST 400",
                            "JURISDICTION_CITY__c": "RIVERSIDE",
                            "JURISDICTION_PHONE__c": "18009528356",
                            "JURISDICTION_STATE__c": "CA",
                            "JURISDICTION_ZIP_CODE__c": "92501",
                            "PHONE_NUMBER__c": "9519283151",
                            "FIRE_ENFORCEMENT_ADDRESS__c": "3737 MAIN ST 400",
                            "FIRE_ENFORCEMENT_CITY__c": "RIVERSIDE",
                            "FIRE_ENFORCEMENT_PHONE__c": "18009528356",
                            "FIRE_ENFORCEMENT_STATE__c": "CA",
                            "FIRE_ENFORCEMENT_ZIP_CODE__c": "92501",
                            "STATUS_ID__c": 2448,
                            "TOTAL_NUMBER_LOTS__c": 223,
                            "ZIP_CODE__c": "92571",
                            "JURISDICTION_NAME__c": "HCD",
                            "NUMBER_RV_LOTS_DRAINS__c": 0,
                            "NUMBER_MH_LOTS__c": 223,
                            "Id": "x0ucs00000EREULAA5"
                        },
                        {
                            "ADDRESS__c": "80 E DAWES ST",
                            "CITY__c": "PERRIS",
                            "COUNTY_NAME__c": "RIVERSIDE",
                            "COUNTY_NUMBER__c": "33",
                            "FIRE_ENFORCEMENT_NAME__c": "HCD - SOUTHERN AREA OFFICE",
                            .......
"""