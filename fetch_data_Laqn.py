import pandas as pd
import requests
import json
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
from src import config as cfg  # Use config for paths

# Configuration
BASE_URL = "https://api.erg.ic.ac.uk/AirQuality"
DATA_DIR = cfg.RAW_AQ
CACHE_DIR = DATA_DIR / "raw_cache"
SITE_FILE = cfg.SITES_POLLUTION
OUTPUT_PARQUET = DATA_DIR / "laqn_2010_2025.parquet"
POLLUTANTS = {"NO2": "NO2", "PM2.5": "PM25"}
REQUEST_TIMEOUT = 30
MAX_WORKERS = 4
CHUNK_SIZE = timedelta(days=90)

# Ensure directories exist
CACHE_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

def create_session():
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def fetch_data_chunk(session, site_code, species_code, start_date, end_date):
    cache_file = CACHE_DIR / f"{site_code}_{species_code}_{start_date}_{end_date}.json"
    if cache_file.exists():
        with open(cache_file, 'r') as f:
            return json.load(f)

    url = f"{BASE_URL}/Data/SiteSpecies/SiteCode={site_code}/SpeciesCode={species_code}/StartDate={start_date}/EndDate={end_date}/Json"
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        with open(cache_file, 'w') as f:
            json.dump(data, f)
        return data
    except Exception as e:
        print(f"Failed chunk {site_code} {species_code} {start_date}-{end_date}: {str(e)}")
        return None

def process_site(session, site_row):
    site_code = site_row["site_code"]
    site_name = site_row["site_name"]
    records = []
    start_date = datetime(2010, 1, 1)
    end_date = datetime(2025, 5, 30)

    current = start_date
    date_chunks = []
    while current < end_date:
        chunk_end = min(current + CHUNK_SIZE, end_date)
        date_chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)

    for pollutant_name, species_code in POLLUTANTS.items():
        for chunk_start, chunk_end in date_chunks:
            str_start = chunk_start.strftime("%Y-%m-%d")
            str_end = chunk_end.strftime("%Y-%m-%d")
            raw = fetch_data_chunk(session, site_code, species_code, str_start, str_end)
            records.extend(extract_records(raw, site_code, site_name, pollutant_name, species_code))

    return records

def extract_records(raw_data, site_code, site_name, pollutant_name, api_code):
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
            try:
                records.append({
                    "site_code": site_code,
                    "site_name": site_name,
                    "pollutant_name": pollutant_name,
                    "pollutant_api_code": api_code,
                    "timestamp": ts,
                    "value": float(val)
                })
            except ValueError:
                continue
    return records

def save_data(records):
    if not records:
        return

    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Filter to business hours: Monday–Friday, 7:00–18:00 UTC
    df = df[df["timestamp"].dt.dayofweek < 5]
    df = df[df["timestamp"].dt.hour.between(7, 18)]

    if not OUTPUT_PARQUET.exists():
        df.to_parquet(OUTPUT_PARQUET, index=False)
    else:
        existing = pd.read_parquet(OUTPUT_PARQUET)
        updated = pd.concat([existing, df], ignore_index=True)
        updated.to_parquet(OUTPUT_PARQUET, index=False)

    print(f"Saved {len(df)} filtered records to {OUTPUT_PARQUET}")

def main():
    site_df = pd.read_csv(SITE_FILE, parse_dates=["overlap_start", "overlap_end"])
    session = create_session()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_site, session, row) for _, row in site_df.iterrows()]
        all_data = []
        for future in as_completed(futures):
            try:
                result = future.result()
                all_data.extend(result)
                print(f"Processed site chunk, total records: {len(all_data)}")
                if len(all_data) % 10000 == 0:
                    save_data(all_data)
                    all_data = []
            except Exception as e:
                print(f"Error processing future: {str(e)}")
        save_data(all_data)

if __name__ == "__main__":
    start_time = time.time()
    main()
    print(f"Completed in {time.time()-start_time:.2f} seconds")
