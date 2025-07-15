import pandas as pd
import requests
import json
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src import config as cfg
from pathlib import Path
from tqdm import tqdm


BASE_URL = "https://api.erg.ic.ac.uk/AirQuality"
DATA_DIR = cfg.RAW_AQ
CACHE_DIR = DATA_DIR / "raw_cache"
OUTPUT_CSV = DATA_DIR / "london_air_quality_2010_2025.csv"

# specify site and pollutant codes
SITES = ["BL0", "BX2", "GN0", "HK6"]
POLLUTANTS = {"NO2": "NO2", "PM2.5": "PM25", "FINE": "FINE"}

REQUEST_TIMEOUT = 30
MAX_WORKERS = 4
CHUNK_SIZE = timedelta(days=90)



def create_session():
    """Creates a requests session with retry logic."""
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def fetch_all_site_metadata(session):
    """
    Fetches metadata for all London sites in a single call and caches the result.
    """
    cache_file = CACHE_DIR / "all_london_sites_metadata.json"
    if cache_file.exists():
        with open(cache_file, 'r') as f:
            return json.load(f)

    url = "https://api.erg.ic.ac.uk/AirQuality/Information/MonitoringSiteSpecies/GroupName=London/Json"
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        all_sites_data = response.json()["Sites"]["Site"]
        
        metadata = {}
        for site in all_sites_data:
            code = site["@SiteCode"]
            metadata[code] = {
                "name": site.get("@SiteName", ""),
                "lat": site.get("@Latitude", None),
                "lon": site.get("@Longitude", None)
            }
        
        with open(cache_file, 'w') as f:
            json.dump(metadata, f)
        return metadata
    except Exception as e:
        print(f"Could not fetch the site metadata list. Error: {e}")
        return None

def fetch_data_chunk(session, site_code, species_code, start_date_str, end_date_str):
    """Fetches and caches a single chunk of air quality data."""
    cache_file = CACHE_DIR / f"{site_code}_{species_code}_{start_date_str}_{end_date_str}.json"
    if cache_file.exists():
        with open(cache_file, 'r') as f:
            return json.load(f)

    url = f"{BASE_URL}/Data/SiteSpecies/SiteCode={site_code}/SpeciesCode={species_code}/StartDate={start_date_str}/EndDate={end_date_str}/Json"
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        with open(cache_file, 'w') as f:
            json.dump(data, f)
        return data
    except Exception:
        return None

def extract_records(raw_data, site_code, site_info, pollutant_name):
    """Extracts and formats data points from the raw JSON response."""
    if not raw_data or "RawAQData" not in raw_data or "Data" not in raw_data["RawAQData"]:
        return []

    data_points = raw_data["RawAQData"]["Data"]
    if not isinstance(data_points, list):
        data_points = [data_points]

    records = []
    for point in data_points:
        ts = point.get("@MeasurementDateGMT")
        val = point.get("@Value")
        if ts and val and val.strip():
            records.append({
                "site_code": site_code,
                "site_name": site_info.get("name"),
                "latitude": site_info.get("lat"),
                "longitude": site_info.get("lon"),
                "pollutant": pollutant_name,
                "timestamp": ts,
                "value": float(val)
            })
    return records

def process_site(session, site_code, site_info):
    """Processes all pollutants and date chunks for a single site."""
    records = []
    start_date = datetime(2010, 1, 1)
    end_date = datetime(2025, 6, 30)

    date_chunks = []
    current = start_date
    while current <= end_date:
        chunk_end = min(current + CHUNK_SIZE, end_date)
        date_chunks.append((current.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        current = chunk_end + timedelta(days=1)

    tasks = [(p_name, s_code, start_str, end_str)
             for p_name, s_code in POLLUTANTS.items()
             for start_str, end_str in date_chunks]

    for pollutant_name, species_code, start_str, end_str in tasks:
        raw = fetch_data_chunk(session, site_code, species_code, start_str, end_str)
        if raw:
            records.extend(extract_records(raw, site_code, site_info, pollutant_name))
    return records

def main():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    session = create_session()
    
    # Fetch all site metadata first
    print("Fetching site metadata...")
    all_sites_metadata = fetch_all_site_metadata(session)
    if not all_sites_metadata:
        return
    print("Metadata loaded.")

    # fetch pollutant data for selected sites
    all_records = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_site = {
            executor.submit(process_site, session, site_code, all_sites_metadata.get(site_code, {})): site_code 
            for site_code in SITES
        }

        for future in tqdm(as_completed(future_to_site), total=len(SITES), desc="Processing Sites"):
            try:
                result = future.result()
                if result:
                    all_records.extend(result)
            except Exception as e:
                site = future_to_site[future]
                print(f"Error processing site {site}: {e}")

    if not all_records:
        print("No new records were fetched.")
        return

    # Convert to DataFrame and save to CSV
    df = pd.DataFrame(all_records)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    df = df.sort_values(by=["site_code", "timestamp"]).reset_index(drop=True)
    
    # Reorder columns
    df = df[['site_code', 'site_name', 'latitude', 'longitude', 'pollutant', 'timestamp', 'value']]
    
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\nSuccessfully saved {len(df)} records to {OUTPUT_CSV}")

if __name__ == "__main__":
    start_time = time.time()
    main()
    print(f"Completed in {time.time() - start_time:.2f} seconds")