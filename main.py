import json
from pathlib import Path
from typing import Any, Optional
import numpy as np
import pandas as pd
import streamlit as st

# Live API fetchers
from scraper.ca_hcd import fetch_ca_hcd
from scraper.rivcoview import fetch_rivcoview
from scraper.mhvillage import fetch_mhvillage_details

# ---------- Helpers ----------
@st.cache_data(show_spinner=False)
def load_json(path: Path) -> Optional[Any]:
    """Load a JSON file from the given path."""
    try:
        if not path.exists():
            st.warning(f"File not found: {path}")
            return None
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        st.error(f"Failed to load or parse {path.name}: {e}")
        return None

@st.cache_data(show_spinner=False)
def load_json_from_url(url: str) -> Optional[Any]:
    """Load JSON data from URL."""
    try:
        import requests
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Failed to load JSON from URL: {e}")
        return None

# ---------- Cached dataset fetchers ----------
@st.cache_data(show_spinner=False)
def cached_ca_hcd(county_code: str = "33") -> Any:
    """Cached CA HCD fetch."""
    return fetch_ca_hcd(county_code)


@st.cache_data(show_spinner=False)
def cached_rivcoview(query_value: str = "Riverside", limit_rows: int | None = 200) -> Any:
    """Cached RivCoView fetch."""
    return fetch_rivcoview(query_value=query_value, limit_rows=limit_rows)


@st.cache_data(show_spinner=False)
def cached_mhvillage_details(county: str = "Riverside", state: str = "CA") -> Any:
    """Cached MHVillage details fetch."""
    return fetch_mhvillage_details(county=county, state=state)

def coerce_numeric(series: pd.Series) -> pd.Series:
    """Coerce a pandas Series to a numeric type, converting errors to NaN."""
    return pd.to_numeric(series, errors="coerce")

def as_dataframe(data: Any) -> pd.DataFrame:
    """Convert raw data (list, dict) into a pandas DataFrame."""
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

def flatten_records_maybe(data: Any) -> list[dict]:
    """Flatten common list-of-list or list-of-dict-of-list shapes."""
    out: list[dict] = []
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                out.append(item)
            elif isinstance(item, list):
                for sub_item in item:
                    if isinstance(sub_item, dict):
                        out.append(sub_item)
            elif isinstance(item, str):
                try:
                    parsed = json.loads(item)
                    if isinstance(parsed, dict):
                        out.append(parsed)
                    elif isinstance(parsed, list):
                        out.extend(d for d in parsed if isinstance(d, dict))
                except json.JSONDecodeError:
                    pass
    elif isinstance(data, dict):
        out.append(data)
    return out

def is_scalar(v: Any) -> bool:
    """Check if a value is a scalar type that can be safely displayed."""
    try:
        if v is None:
            return True
        if pd.isna(v):
            return True
        if isinstance(v, (str, int, float, bool, bytes)):
            return True
        if np.isscalar(v):
            return True
        if isinstance(v, np.ndarray):
            if v.size == 0:
                return False
            return v.ndim == 0
        if isinstance(v, pd.Series):
            return False
        if isinstance(v, (list, tuple)):
            return False
        if isinstance(v, dict):
            return False
        if hasattr(v, '__len__'):
            return False
        return True
    except Exception:
        return False

def safe_json_dumps(v: Any) -> str:
    """Safely convert a value to JSON string."""
    try:
        if isinstance(v, np.ndarray):
            return json.dumps(v.tolist(), ensure_ascii=False)
        elif isinstance(v, pd.Series):
            return json.dumps(v.tolist(), ensure_ascii=False)
        else:
            return json.dumps(v, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(v)

def sanitize_for_arrow(df: pd.DataFrame, prefer_cols: Optional[list[str]] = None, max_cols: int = 20) -> pd.DataFrame:
    """Prepare a DataFrame for safe rendering with Streamlit's st.dataframe/st.map."""
    if df.empty:
        return df

    cleaned = df.copy()
    for col in cleaned.columns:
        s = cleaned[col]
        if pd.api.types.is_numeric_dtype(s) or pd.api.types.is_bool_dtype(s) or pd.api.types.is_string_dtype(s):
            continue
        
        coerced_numeric = pd.to_numeric(s, errors='coerce')
        if len(s.dropna()) > 0 and coerced_numeric.notna().sum() / len(s.dropna()) > 0.9:
             cleaned[col] = coerced_numeric
        else:
            cleaned[col] = s.apply(lambda v: safe_json_dumps(v) if not is_scalar(v) else v)

    safe_cols = []
    for c in cleaned.columns:
        col_data = cleaned[c]
        if (pd.api.types.is_numeric_dtype(col_data) or 
            pd.api.types.is_bool_dtype(col_data) or 
            pd.api.types.is_string_dtype(col_data) or
            pd.api.types.is_datetime64_any_dtype(col_data)):
            safe_cols.append(c)
    
    ordered_cols = []
    if prefer_cols:
        ordered_cols.extend([c for c in prefer_cols if c in safe_cols])
    
    ordered_cols.extend([c for c in safe_cols if c not in ordered_cols])
    return cleaned[ordered_cols[:max_cols]] if ordered_cols else cleaned.iloc[:, :max_cols]

def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to be more readable."""
    df = df.copy()
    
    # Column name mappings for better readability
    column_mappings = {
        'payload.name': 'Community Name',
        'payload.relationships.address.streetAddress1': 'Street Address',
        'payload.relationships.address.city': 'City',
        'payload.relationships.address.state': 'State',
        'payload.relationships.address.postalCode': 'Zip Code',
        'payload.relationships.address.coordinatePoint.latitude': 'Latitude',
        'payload.relationships.address.coordinatePoint.longitude': 'Longitude',
        'payload.relationships.address.county': 'County',
        'payload.relationships.siteCount.total': 'Total Sites',
        'payload.relationships.siteCount.vacant': 'Vacant Sites',
        'payload.relationships.homesCount.forSaleCount': 'Homes For Sale',
        'payload.relationships.homesCount.forRentCount': 'Homes For Rent',
        'payload.relationships.phone.number': 'Phone Number',
        'payload.relationships.favoriteCount.total': 'Favorite Count',
        'payload.averageMonthlyRent': 'Avg Monthly Rent',
        'payload.ageRestrictions': 'Age Restrictions',
        'payload.ageRestrictionsDescription': 'Age Restrictions Description',
        'payload.petsAllowed': 'Pets Allowed',
        'payload.isResidentOwned': 'Resident Owned',
        'payload.yearBuilt': 'Year Built',
        'payload.caption': 'Caption',
        'payload.description': 'Description',
        'payload.website': 'Website',
        'payload.virtualTour': 'Virtual Tour'
    }
    
    # Apply mappings
    df = df.rename(columns=column_mappings)
    
    # Clean up remaining column names
    new_columns = {}
    for col in df.columns:
        if col not in column_mappings.values():  # Only process unmapped columns
            # Remove payload.relationships. prefix and make readable
            clean_col = col.replace('payload.relationships.', '').replace('payload.', '')
            clean_col = clean_col.replace('.', ' ').replace('_', ' ')
            # Capitalize words
            clean_col = ' '.join(word.capitalize() for word in clean_col.split())
            new_columns[col] = clean_col
    
    df = df.rename(columns=new_columns)
    return df

def extract_amenities(row: pd.Series) -> dict:
    """Extract amenity information from the details array.

    Supports both raw column name 'payload.relationships.details' and
    normalized 'Details'. Returns flat columns like 'amenity_Golf Course': 'Yes/No'.
    """
    # Locate details list in either raw or normalized column
    details = row.get('payload.relationships.details', None)
    if details is None:
        details = row.get('Details', None)
    if not isinstance(details, list):
        return {}

    import re

    def to_readable(name: str) -> str:
        # Replace underscores, split camelCase like "golfCourse" -> "golf Course"
        name = name.replace('_', ' ')
        name = re.sub(r'([a-z0-9])([A-Z])', r'\1 \2', name)
        name = name.replace('  ', ' ').strip()
        return name.title()

    amenities: dict[str, Any] = {}
    amenity_categories = {'amenity', 'infrastructure', 'location'}

    for detail in details:
        if isinstance(detail, dict):
            category = detail.get('category', '')
            detail_type = str(detail.get('type', '') or '')
            value = detail.get('value')

            if category in amenity_categories and value is not None and detail_type:
                # Convert boolean values to Yes/No for better readability
                if isinstance(value, bool):
                    value = 'Yes' if value else 'No'
                # Skip empty strings
                if isinstance(value, str) and value.strip() == '':
                    continue
                # Create readable names for amenities
                readable_name = to_readable(detail_type)
                amenities[f'amenity_{readable_name}'] = value

    return amenities

# ---------- File location helpers ----------
def find_dataset_file(dataset: str) -> Optional[Path]:
    """Return the first existing path for a dataset's results.json."""
    here = Path(__file__).resolve().parent
    candidates = [
        Path.cwd() / "data" / dataset / "results.json",
        here / "data" / dataset / "results.json",
        Path("data") / dataset / "results.json",
        Path("/data") / f"{dataset}_results.json",
    ]

    for p in candidates:
        try:
            if p.exists():
                return p
        except Exception:
            continue
    return None

# ---------- Page ----------
st.set_page_config(page_title="Scraped Data Explorer", layout="wide")
st.title("Scraped Data Explorer")
st.caption("Browse and analyze datasets collected by the scrapers.")

# (Removed Upload New Dataset UI)

# ---------- Sidebar for dataset selection ----------
with st.sidebar:
    st.header("Datasets (Live API)")
    available = ["CA HCD", "RivCoView", "MHVillage"]
    chosen = st.multiselect(
        "Select datasets",
        options=available,
        default=available,
        key="datasets_select",
    )
    st.caption("Data is fetched live from source APIs on each run.")
    st.divider()

def section_header(
    label: str,
    source: Optional[str] = None,
    refresh_key: Optional[str] = None,
    on_refresh: Optional[callable] = None,
):
    """Render a section header with optional per-dataset refresh button."""
    cols = st.columns([1, 2, 1])
    with cols[0]:
        st.subheader(label)
    with cols[1]:
        st.caption(f"Source: {source or 'Live API'} | Cached; use Refresh to re-fetch")
    with cols[2]:
        if refresh_key and st.button("Refresh data", key=refresh_key):
            if on_refresh:
                try:
                    on_refresh()
                finally:
                    pass
            st.toast("Cache cleared. Re-fetching…", icon="🔄")

    

# ---------- RivCoView parsing helpers ----------
def _num(s: Any) -> Optional[float]:
    """Safely convert a value to a float, handling currency and commas."""
    if pd.isna(s):
        return None
    if isinstance(s, (int, float)):
        return float(s)
    if isinstance(s, str):
        try:
            return float(s.replace("$", "").replace(",", "").strip())
        except (ValueError, TypeError):
            return None
    return None

def _to_dt(s: Any) -> Optional[pd.Timestamp]:
    """Safely convert a value to a pandas Timestamp."""
    if not s or pd.isna(s):
        return None
    return pd.to_datetime(s, errors="coerce")

def enrich_rivco_row(row: pd.Series) -> dict:
    """Extract and summarize nested sales and history data for a RivCoView row."""
    out: dict[str, Any] = {}
    
    sales = row.get("sales", [])
    if isinstance(sales, list) and sales:
        sales_dicts = [s for s in sales if isinstance(s, dict)]
        out["salesCount"] = len(sales_dicts)
        
        last_sale = max(sales_dicts, key=lambda s: _to_dt(s.get("saledate")) or pd.Timestamp.min, default=None)
        
        if last_sale:
            out["lastSaleDate"] = last_sale.get("saledate")
            out["lastSalePrice"] = _num(last_sale.get("SalePrice") or last_sale.get("salePrice"))
            out["lastSaleQualified"] = last_sale.get("Qualified") or last_sale.get("qualified")
    else:
        out["salesCount"] = 0

    hist = row.get("history", [])
    if isinstance(hist, list) and hist:
        hist_items = sorted(
            [h for h in hist if isinstance(h, dict)],
            key=lambda h: int(h.get("TaxYear") or h.get("taxYear") or 0)
        )
        if hist_items:
            latest = hist_items[-1]
            prev = hist_items[-2] if len(hist_items) > 1 else None
            
            latest_val = _num(latest.get("AssessedTot") or latest.get("assessedTot"))
            prev_val = _num(prev.get("AssessedTot") or prev.get("assessedTot")) if prev else None
            
            out["assessedYearLatest"] = str(latest.get("TaxYear") or latest.get("taxYear"))
            out["assessedLatest"] = latest_val
            out["assessedPrev"] = prev_val
            
            if latest_val is not None and prev_val is not None and prev_val != 0:
                out["assessedYoYDelta"] = latest_val - prev_val
                out["assessedYoYPct"] = (latest_val - prev_val) / prev_val * 100.0

    return out

# ---------- Render Functions for Each Dataset ----------
def render_ca_hcd():
    # Header with per-dataset refresh
    section_header(
        "CA HCD (Parks)",
        "Live API",
        refresh_key="refresh_ca_hcd",
        on_refresh=lambda: cached_ca_hcd.clear(),
    )
    with st.spinner("Fetching CA HCD parks..."):
        data = cached_ca_hcd()
    df = as_dataframe(data)

    if df.empty:
        st.info("No CA HCD data to display.")
        return

    numeric_cols = ["totalNumberLots", "numberMhLots", "numberRvLotsDrains", "numberRvLotsNoDrains", "statusId"]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = coerce_numeric(df[c])

    cols = st.columns(3)
    with cols[0]:
        cities = sorted(df["city"].dropna().unique()) if "city" in df.columns else []
        sel_cities = st.multiselect("City", options=cities, key="ca_hcd_city")
    with cols[1]:
        statuses = sorted(df["statusId"].dropna().unique()) if "statusId" in df.columns else []
        sel_status = st.multiselect("StatusId", options=statuses, key="ca_hcd_status")
    with cols[2]:
        if "totalNumberLots" in df.columns:
            min_lots, max_lots = int(df["totalNumberLots"].min()), int(df["totalNumberLots"].max())
            lot_range = st.slider("Total lots range", min_lots, max_lots, (min_lots, max_lots), key="ca_hcd_lots")
        else:
            lot_range = (0, 0)
    
    fdf = df.copy()
    if sel_cities:
        fdf = fdf[fdf["city"].isin(sel_cities)]
    if sel_status:
        fdf = fdf[fdf["statusId"].isin(sel_status)]
    if "totalNumberLots" in fdf.columns and lot_range != (0, 0):
        fdf = fdf[fdf["totalNumberLots"].between(lot_range[0], lot_range[1])]

    

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Parks", f"{len(fdf):,}")
    
    total_lots = fdf["totalNumberLots"].sum() if "totalNumberLots" in fdf.columns else 0
    k2.metric("Total lots", f"{int(total_lots):,}")
    
    mh_lots = fdf["numberMhLots"].sum() if "numberMhLots" in fdf.columns else 0
    k3.metric("MH lots", f"{int(mh_lots):,}")
    
    rv_lots = fdf["numberRvLotsDrains"].sum() if "numberRvLotsDrains" in fdf.columns else 0
    k4.metric("RV lots (drains)", f"{int(rv_lots):,}")

    if "city" in fdf.columns and not fdf.empty:
        grp = fdf.groupby("city")["totalNumberLots"].sum().sort_values(ascending=False).rename("lots")
        st.bar_chart(grp)

    show_cols = [c for c in [
        "parkName", "address", "city", "zipCode", "parkIdentifier", "phoneNumber", 
        "totalNumberLots", "numberMhLots", "numberRvLotsDrains", "statusId"
    ] if c in fdf.columns]
    st.dataframe(sanitize_for_arrow(fdf, prefer_cols=show_cols), use_container_width=True)

def render_rivcoview():
    # Header with per-dataset refresh
    section_header(
        "RivCoView (Assessor details)",
        "Live API",
        refresh_key="refresh_rivco",
        on_refresh=lambda: cached_rivcoview.clear(),
    )
    with st.spinner("Fetching RivCoView parcels..."):
        data = cached_rivcoview()
    flat = flatten_records_maybe(data)
    df = as_dataframe(flat)

    if df.empty:
        st.info("No RivCoView data to display.")
        return

    for c in ["lat", "lng", "taxTotal"]:
        if c in df.columns:
            df[c] = coerce_numeric(df[c])
    df = df.rename(columns={"class_code": "classCode"})

    cols = st.columns(3)
    # Prefer new 'city' field appended by scraper; fallback to legacy columns
    city_col = (
        "city" if "city" in df.columns else (
            "situs_city" if "situs_city" in df.columns else (
                "situsCity" if "situsCity" in df.columns else None
            )
        )
    )
    with cols[0]:
        cities = sorted(df[city_col].dropna().unique()) if city_col and city_col in df.columns else []
        sel_cities = st.multiselect("City", options=cities, key="rivco_city")
    with cols[1]:
        classes = sorted(df["classCode"].dropna().unique()) if "classCode" in df.columns else []
        sel_class = st.multiselect("Class code", options=classes, key="rivco_class")
    with cols[2]:
        has_geo = st.checkbox("Only with coordinates", value=True, key="rivco_has_geo")

    fdf = df.copy()
    if sel_cities and city_col and city_col in fdf.columns:
        fdf = fdf[fdf[city_col].isin(sel_cities)]
    if sel_class and "classCode" in fdf.columns:
        fdf = fdf[fdf["classCode"].isin(sel_class)]
    if has_geo and all(c in fdf.columns for c in ["lat", "lng"]):
        fdf = fdf[fdf["lat"].notna() & fdf["lng"].notna()]

    
    
    enrich_df = fdf.copy()
    if not enrich_df.empty:
        extras = enrich_df.apply(enrich_rivco_row, axis=1, result_type="expand")
        enrich_df = pd.concat([enrich_df, extras], axis=1)

    c1, c2, c3 = st.columns(3)
    c1.metric("Parcels", f"{len(fdf):,}")
    c2.metric("With coordinates", f"{int(fdf['lat'].notna().sum()) if 'lat' in fdf.columns else 0:,}")
    avg_assessed = enrich_df["assessedLatest"].mean() if "assessedLatest" in enrich_df.columns else 0
    c3.metric("Avg assessed total (latest)", f"${avg_assessed:,.2f}" if avg_assessed else "—")
    
    if all(c in fdf.columns for c in ["lat", "lng"]) and not fdf.empty:
        map_df = fdf[["lat", "lng"]].dropna()
        if not map_df.empty:
            st.map(map_df.rename(columns={"lng": "lon"}), use_container_width=True)

    show_cols = [c for c in [
        "apn", "address", city_col, "classCode", "taxTotal", "acreage", "lat", "lng",
        "salesCount", "lastSaleDate", "lastSalePrice", "assessedYearLatest", "assessedLatest",
        "assessedPrev", "assessedYoYDelta", "assessedYoYPct"
    ] if (c is not None and c in enrich_df.columns)]
    st.dataframe(sanitize_for_arrow(enrich_df, prefer_cols=show_cols), use_container_width=True)

def render_mhvillage():
    # Header with per-dataset refresh
    section_header(
        "MHVillage (Park details)",
        "Live API",
        refresh_key="refresh_mhvillage",
        on_refresh=lambda: cached_mhvillage_details.clear(),
    )
    with st.spinner("Fetching MHVillage communities..."):
        data = cached_mhvillage_details()
    df = as_dataframe(data)

    if df.empty:
        st.info("No MHVillage data to display.")
        return

    # Normalize human-friendly columns
    df = normalize_column_names(df)

    # Extract amenities into flat columns
    if not df.empty:
        amenities_data = df.apply(extract_amenities, axis=1, result_type="expand")
        if amenities_data is not None and not amenities_data.empty:
            df = pd.concat([df, amenities_data], axis=1)

    # Convert relevant numeric fields
    numeric_cols = [
        'Total Sites', 'Vacant Sites', 'Homes For Sale', 'Homes For Rent',
        'Latitude', 'Longitude', 'Avg Monthly Rent', 'Favorite Count'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = coerce_numeric(df[col])

    # Filters
    cols = st.columns(3)
    with cols[0]:
        cities = sorted(df['City'].dropna().unique()) if 'City' in df.columns else []
        sel_cities = st.multiselect("City", options=cities, key="mh_city")
    with cols[1]:
        if 'Total Sites' in df.columns and df['Total Sites'].notna().any():
            min_sites = int(df['Total Sites'].min())
            max_sites = int(df['Total Sites'].max())
            site_range = st.slider("Total sites", min_sites, max_sites, (min_sites, max_sites), key="mh_site_range")
        else:
            site_range = (0, 0)
    with cols[2]:
        has_geo = st.checkbox("Only with coordinates", value=False, key="mh_has_geo")

    fdf = df.copy()
    if sel_cities and 'City' in fdf.columns:
        fdf = fdf[fdf['City'].isin(sel_cities)]
    if 'Total Sites' in fdf.columns and site_range != (0, 0):
        fdf = fdf[fdf['Total Sites'].between(site_range[0], site_range[1])]
    if has_geo and all(c in fdf.columns for c in ['Latitude', 'Longitude']):
        fdf = fdf[fdf['Latitude'].notna() & fdf['Longitude'].notna()]

    

    # KPIs
    k1, k2, k3 = st.columns(3)
    k1.metric("Communities", f"{len(fdf):,}")

    total_sites_sum = fdf['Total Sites'].sum() if 'Total Sites' in fdf.columns else 0
    k2.metric("Total sites", f"{int(total_sites_sum):,}")

    photos_metric = 0
    if 'Photos' in fdf.columns:
        photos_metric = int(fdf['Photos'].apply(lambda v: isinstance(v, list) and len(v) > 0).sum())
    k3.metric("With photos", f"{photos_metric:,}")

    # Map
    if all(c in fdf.columns for c in ['Latitude', 'Longitude']) and not fdf.empty:
        map_df = fdf[['Latitude', 'Longitude']].dropna()
        if not map_df.empty:
            st.map(map_df.rename(columns={"Latitude": "lat", "Longitude": "lon"}), use_container_width=True)

    # Relevant display columns
    priority_cols = [
        'Community Name', 'Street Address', 'City', 'State', 'County', 'Zip Code',
        'Total Sites', 'Vacant Sites', 'Homes For Sale', 'Homes For Rent',
        'Avg Monthly Rent', 'Phone Number', 'Age Restrictions Description',
        'Favorite Count', 'Website', 'Caption', 'Description'
    ]
    show_cols = [c for c in priority_cols if c in fdf.columns]

    # Add a few popular amenities if present
    key_amenities = [
        'amenity_Golf Course', 'amenity_Swimming Pool', 'amenity_Clubhouse',
        'amenity_Fitness Center', 'amenity_Pickleball', 'amenity_Gated',
        'amenity_Waterfront', 'amenity_Shuffleboard Court'
    ]
    show_cols.extend([col for col in key_amenities if col in fdf.columns])
    
    st.subheader("Community Details")
    st.dataframe(sanitize_for_arrow(fdf, prefer_cols=show_cols, max_cols=25), use_container_width=True)

# ---------- Main render loop ----------
if "CA HCD" in chosen:
    render_ca_hcd()
    st.divider()

if "RivCoView" in chosen:
    render_rivcoview()
    st.divider()

if "MHVillage" in chosen:
    render_mhvillage()
    st.divider()


st.caption("Tip: use the sidebar to filter datasets.")
