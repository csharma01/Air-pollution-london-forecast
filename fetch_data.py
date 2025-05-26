#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import requests
from datetime import datetime, timedelta
import os


# In[3]:


# Base API URL and paths
BASE_URL = "https://api.erg.ic.ac.uk/AirQuality"
DATA_DIR = "src/data"
SITE_FILE = os.path.join(DATA_DIR, "selected_sites.csv")
OUTPUT_CSV = os.path.join(DATA_DIR, "laqn_sample.csv")
OUTPUT_PARQUET = os.path.join(DATA_DIR, "laqn_sample.parquet")

# Pollutants to fetch
POLLUTANTS = {
    "NO2": "NO2",
    "PM2.5": "PM25"
}


# In[6]:


# Load top 10 sites with overlap metadata
site_df = pd.read_csv(SITE_FILE, parse_dates=["overlap_start", "overlap_end"])
print(f"Loaded {len(site_df)} selected sites for sampling:")
site_df[["site_code", "site_name", "overlap_start", "overlap_end", "overlap_years"]]


# In[8]:


def fetch_hourly_data(site_code, species_code, start_date, end_date):
    url = f"{BASE_URL}/Data/SiteSpecies/SiteCode={site_code}/SpeciesCode={species_code}/StartDate={start_date}/EndDate={end_date}/Json"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Failed {site_code} {species_code}: {e}")
        return None


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


# In[10]:


def has_enough_data(site_code, species_code, start_date, end_date, min_records=100):
    """Fetches quick sample and returns True if enough records exist."""
    url = f"{BASE_URL}/Data/SiteSpecies/SiteCode={site_code}/SpeciesCode={species_code}/StartDate={start_date}/EndDate={end_date}/Json"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        records = data.get("RawAQData", {}).get("Data", [])
        if isinstance(records, dict):
            records = [records]
        valid_values = [d for d in records if "@Value" in d and d["@Value"].strip()]
        return len(valid_values) >= min_records
    except Exception as e:
        print(f"Validation failed for {site_code} {species_code}: {e}")
        return False


# In[15]:


SAMPLE_DAYS = 7
MIN_RECORDS = 100
all_data = []

for _, row in site_df.iterrows():
    site_code = row["site_code"]
    site_name = row["site_name"]
    overlap_start = row["overlap_start"]
    overlap_end = row["overlap_end"]

    # Define window
    sample_start_dt = overlap_start + timedelta(days=30)
    sample_end_dt = min(sample_start_dt + timedelta(days=SAMPLE_DAYS - 1), overlap_end)

    sample_start = sample_start_dt.strftime("%Y-%m-%d")
    sample_end = sample_end_dt.strftime("%Y-%m-%d")

    # Validate data exists in full range
    valid_no2 = has_enough_data(site_code, "NO2", sample_start, sample_end, MIN_RECORDS)
    valid_pm25 = has_enough_data(site_code, "PM25", sample_start, sample_end, MIN_RECORDS)

    if not (valid_no2 and valid_pm25):
        print(f"Skipping {site_code} ({site_name}) - insufficient long-range data.")
        continue

    # Fetch and store
    for pollutant_name, species_code in POLLUTANTS.items():
        print(f"Fetching {pollutant_name} for {site_code} ({site_name}) from {sample_start} to {sample_end}...")
        raw = fetch_hourly_data(site_code, species_code, sample_start, sample_end)
        records = extract_records(raw, site_code, site_name, pollutant_name, species_code)
        all_data.extend(records)
        print(f"{len(records)} records added.")


# In[17]:


# Convert to DataFrame
df = pd.DataFrame(all_data)
df["timestamp"] = pd.to_datetime(df["timestamp"])

# Optional reordering
df = df[["site_code", "site_name", "pollutant_name", "pollutant_api_code", "timestamp", "value"]]

# Save both formats
os.makedirs(DATA_DIR, exist_ok=True)
df.to_csv(OUTPUT_CSV, index=False)
df.to_parquet(OUTPUT_PARQUET, index=False)

print(f"\nSaved {len(df)} rows")
print(f"- CSV: {OUTPUT_CSV}")
print(f"- Parquet: {OUTPUT_PARQUET}")


# In[19]:


# Load from saved CSV
df_sample = pd.read_csv("src/data/laqn_sample.csv", parse_dates=["timestamp"])

# Preview
print("Sample preview:")
display(df_sample.head())

# Dimensions
print(f"\nTotal records: {len(df_sample)}")
print(f"Unique sites: {df_sample['site_code'].nunique()}")
print(f"Pollutants: {df_sample['pollutant_name'].unique()}")


# In[21]:


# Basic description
print("Summary statistics for 'value':")
display(df_sample["value"].describe())

# Missing values
print("\nMissing values by column:")
display(df_sample.isnull().sum())

# Record count by site and pollutant
print("\nRecord count by site and pollutant:")
display(df_sample.groupby(["site_code", "pollutant_name"]).size().unstack())


# In[ ]:




