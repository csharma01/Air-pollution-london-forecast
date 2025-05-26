#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from datetime import datetime
import pandas as pd
import os


# In[3]:


def get_dual_pollutant_sites():
    url = "https://api.erg.ic.ac.uk/AirQuality/Information/MonitoringSiteSpecies/GroupName=London/Json"
    response = requests.get(url)
    response.raise_for_status()

    all_sites = response.json()["Sites"]["Site"]
    results = []

    for site in all_sites:
        code = site["@SiteCode"]
        name = site["@SiteName"]
        lat = site.get("@Latitude", "")
        lon = site.get("@Longitude", "")
        opened = site.get("@DateOpened", "")
        closed = site.get("@DateClosed", "")

        species = site.get("Species", [])
        if isinstance(species, dict):
            species = [species]

        # Look for both NO2 and PM2.5
        no2_start = no2_end = pm25_start = pm25_end = None
        for pollutant in species:
            if pollutant["@SpeciesCode"] == "NO2":
                no2_start = pollutant.get("@DateMeasurementStarted")
                no2_end = pollutant.get("@DateMeasurementFinished")
            elif pollutant["@SpeciesCode"] == "PM25":
                pm25_start = pollutant.get("@DateMeasurementStarted")
                pm25_end = pollutant.get("@DateMeasurementFinished")

        if no2_start and pm25_start:
            results.append({
                "site_code": code,
                "site_name": name,
                "latitude": lat,
                "longitude": lon,
                "date_opened": opened,
                "date_closed": closed,
                "no2_start": no2_start,
                "no2_end": no2_end,
                "pm25_start": pm25_start,
                "pm25_end": pm25_end
            })

    return pd.DataFrame(results)


# In[5]:


def calculate_overlap(row):
    try:
        def parse_or_now(date_str):
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S") if date_str else datetime.today()

        no2_start = parse_or_now(row["no2_start"])
        no2_end = parse_or_now(row["no2_end"])
        pm25_start = parse_or_now(row["pm25_start"])
        pm25_end = parse_or_now(row["pm25_end"])

        overlap_start = max(no2_start, pm25_start)
        overlap_end = min(no2_end, pm25_end)

        if overlap_end < overlap_start:
            return pd.Series({
                "overlap_start": None,
                "overlap_end": None,
                "overlap_years": 0
            })

        overlap_years = (overlap_end - overlap_start).days / 365.25

        return pd.Series({
            "overlap_start": overlap_start,
            "overlap_end": overlap_end,
            "overlap_years": round(overlap_years, 2)
        })

    except Exception as e:
        print(f"Error calculating overlap: {e}")
        return pd.Series({
            "overlap_start": None,
            "overlap_end": None,
            "overlap_years": 0
        })


# In[7]:


# Step 1: Get raw site data
df_sites = get_dual_pollutant_sites()

# Step 2: Calculate overlap
df_sites[["overlap_start", "overlap_end", "overlap_years"]] = df_sites.apply(calculate_overlap, axis=1)

# Step 3: Filter and sort
top_sites = df_sites[df_sites["overlap_years"] >= 5].sort_values(by="overlap_years", ascending=False).head(10)

# Step 4: View the result
top_sites_summary = top_sites[["site_code", "site_name", "overlap_start", "overlap_end", "overlap_years"]]
top_sites_summary.reset_index(drop=True, inplace=True)
top_sites_summary


# In[9]:


output_path = "src/data/selected_sites.csv"
top_sites_summary.to_csv(output_path, index=False)
print(f"Saved top 10 valid sites to {output_path}")


# In[ ]:




