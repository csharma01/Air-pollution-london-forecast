import pandas as pd
import openmeteo_requests
import requests_cache
import time
from retry_requests import retry
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from src import config as cfg

# setup the api client with a cache and retry session
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

def fetch_weather_for_site(site_info):
    """
    worker function to fetch and save weather data for a single site.
    """
    site_code = site_info["site_code"]
    lat = site_info["laqn_lat"]
    lon = site_info["laqn_lon"]
    
    output_dir = cfg.RAW_WTH
    output_file = output_dir / f"weather_{site_code}.csv"
    
    if output_file.exists():
        return f"skipped: data for {site_code} already exists."

    # define api parameters
    url = "https://archive-api.open-meteo.com/v1/archive"
    hourly_variables = [
        "temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation",
        "snow_depth", "weather_code", "pressure_msl", "cloud_cover",
        "shortwave_radiation", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m"
    ]
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": "2010-01-01",
        "end_date": "2025-06-29",
        "hourly": hourly_variables,
        "timeformat": "iso8601",
        "timezone": "GMT"
    }

    try:
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]
        hourly = response.Hourly()

        # generate a full date range
        hourly_data = {"time": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )}
        
        # process the response into a dataframe
        for i, var in enumerate(hourly_variables):
            hourly_data[var] = hourly.Variables(i).ValuesAsNumpy()
        
        hourly_dataframe = pd.DataFrame(data=hourly_data)
        
        # save the data
        hourly_dataframe.to_csv(output_file, index=False)
        return f"success: saved data for {site_code}."

    except Exception as e:
        return f"error: could not process {site_code}. reason: {e}"

def main():
    try:
        sites_df = pd.read_csv(cfg.MATCHED_SITES)
    except FileNotFoundError:
        print(f"error: site mapping file not found at {cfg.MATCHED_SITES}")
        return

    site_list = sites_df.to_dict('records')

    output_dir = cfg.RAW_WTH
    output_dir.mkdir(parents=True, exist_ok=True)
    
    max_workers = 4
    
    print(f"starting weather data fetch for {len(site_list)} sites using {max_workers} workers...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for site in site_list:
            # submit a new task to the executor
            future = executor.submit(fetch_weather_for_site, site)
            futures[future] = site['site_code']
            # add a short delay between starting each thread to avoid rate-limiting
            time.sleep(1) 
        
        for future in tqdm(as_completed(futures), total=len(site_list), desc="fetching weather data"):
            try:
                result = future.result()
            except Exception as e:
                print(f"a worker failed with an unexpected error: {e}")

    print("\nweather data fetching process complete.")

if __name__ == "__main__":
    main()