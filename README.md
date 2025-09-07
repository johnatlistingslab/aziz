# Aziz

## Streamlit UI

An interactive Streamlit app to browse and analyze data fetched live from source APIs.

How to run (Windows PowerShell):

```powershell
# (optional) create and activate a virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# install runtime dependencies
pip install -U pip
pip install streamlit curl-cffi

# launch the app
streamlit run .\main.py
```

What it does:

- Live sources in the sidebar: CA HCD, RivCoView, MHVillage (all fetched on demand).
- Smart display: converts nested JSON to tables, coerces numeric types safely, and hides unsafe columns for Streamlit’s table engine.
- Quick KPIs and visuals per dataset (counts, sums/averages, small charts/maps where available).

Dataset-specific features:

- CA HCD (Mobile Home Parks)
	- Filters: City, StatusId, Total lots range.
	- KPIs: Parks count, Total lots, MH lots, RV lots (drains).
	- Chart: Total lots by City.
	- Table: Common park fields (name, address, phone, lots, status).

- RivCoView (Assessor details)
	- Filters: City, Class code, Only with coordinates.
	- Enrichment: salesCount, lastSaleDate/lastSalePrice; assessedYearLatest, assessedLatest/assessedPrev, YoY delta and %.
	- Map: lat/lng points if present.
	- Table: APN, address, classCode, taxes, acreage, geo, plus enrichment.

- MHVillage (Park details)
	- Normalizes column names to human-friendly labels (e.g., Community Name, Total Sites, Avg Monthly Rent).
	- Extracts amenities from nested details into flat amenity_* columns (e.g., amenity_Golf Course).
	- Filters: City, Total sites range, Only with coordinates.
	- KPIs: Communities count, Total sites, With photos.
	- Map: plots communities with coordinates.
	- Table: important park details and popular amenities.


## Sources

The following official sources are used for scraping and live data:

- **CA HCD Mobilehome/RV Park Search (official):**  
	https://cahcd.my.site.com/s/mobilehomeparksearch
- **MHVillage — Riverside County parks directory:**  
	https://www.mhvillage.com/parks/ca/riverside-county
- **RivCoView — Assessor/Property portal:**  
	https://rivcoview.rivcoacr.org/

## CLI scraping

Run the scrapers from the command line and save to JSON/CSV.

Examples (Windows PowerShell):

```powershell
# CA HCD (Riverside county code 33)
python .\scrape.py --source ca_hcd --county "Riverside" --limit 200 --out data\parks.json

# MHVillage (county+state)
python .\scrape.py --source mhvillage --county "Riverside" --state CA --limit 200 --out data\mhvillage.csv

# RivCoView (searches by situs city/address containing county/city text)
python .\scrape.py --source rivcoview --county "Riverside" --limit 200 --out data\rivco.json
```

Notes:

- Output format is inferred from the extension: .json or .csv.
- For CA HCD, the county name is mapped to a county code (Riverside => 33). If your county isn't recognized, pass `--county-code` explicitly.
