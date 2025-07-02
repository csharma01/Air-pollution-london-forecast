# C:\Air pollution london\fetch_weather_data.py

import pandas as pd
import openmeteo_requests
import requests_cache
import time
from retry_requests import retry
from src import config as cfg


def fetch_all_weather_data(sample_size=None):

    # setup the api client with a cache
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # define all required parameters for the api call
    url = "https://archive-api.open-meteo.com/v1/archive"
    start_date = "2010-01-01"
    end_date = "2025-05-29"

    hourly_variables = [
        "temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation",
        "snow_depth", "weather_code", "pressure_msl", "cloud_cover",
        "shortwave_radiation", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m"
    ]

    try:
        sites_df = pd.read_csv(cfg.MATCHED_SITES)
    except FileNotFoundError:
        print(f"error: site mapping file not found at {cfg.MATCHED_SITES}")
        return

    # apply sample size if specified
    if sample_size:
        sites_df = sites_df.head(sample_size)

    output_dir = cfg.RAW_WTH
    output_dir.mkdir(parents=True, exist_ok=True)

    # loop through each site and fetch its data
    for index, site in sites_df.iterrows():
        site_code = site["site_code"]
        lat = site["laqn_lat"]
        lon = site["laqn_lon"]

        output_file = output_dir / f"weather_{site_code}.csv"

        if output_file.exists():
            print(f"data for site {site_code} already exists. skipping.")
            continue

        print(f"requesting data for site: {site_code} ({index + 1}/{len(sites_df)})...")

        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": hourly_variables,
            "timeformat": "iso8601",
            "timezone": "GMT"
        }

        try:
            responses = openmeteo.weather_api(url, params=params)
            response = responses[0]

            hourly = response.Hourly()

            # generate the hourly timestamp range
            hourly_data = {"time": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            )}

            for i, var in enumerate(hourly_variables):
                hourly_data[var] = hourly.Variables(i).ValuesAsNumpy()

            hourly_dataframe = pd.DataFrame(data=hourly_data)

            hourly_dataframe.to_csv(output_file, index=False)
            print(f"successfully saved data for {site_code} to {output_file}")

        except Exception as e:
            print(f"could not fetch or process data for site {site_code}. error: {e}")

        # standard delay between each request
        print("waiting for 5 seconds...")
        time.sleep(5)

        # pause every 3 sites to avoid minutely rate limits
        is_batch_end = (index + 1) % 3 == 0
        is_not_last_item = (index + 1) < len(sites_df)

        if is_batch_end and is_not_last_item:
            print("\nbatch of 3 complete. pausing for 60 seconds to respect rate limits...")
            time.sleep(60)

    print("\nweather data fetching process complete.")


if __name__ == "__main__":
    fetch_all_weather_data(sample_size=None)