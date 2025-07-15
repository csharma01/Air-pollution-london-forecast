# C:\Air pollution london\prepare_weather_data.py

import pandas as pd
from src import config as cfg

def prepare_full_weather_data():
    
    # define input and output directories
    input_dir = cfg.RAW_WTH
    output_dir = cfg.INT_WTH
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "weather_combined_full.csv"

    weather_files = list(input_dir.glob("weather_*.csv"))

    if not weather_files:
        print(f"error: no weather files were found in {input_dir}")
        return

    print(f"found {len(weather_files)} weather files to combine.")

    all_weather_dfs = []
    for file_path in weather_files:
        try:
            # extract site_code from filename
            site_code = file_path.stem.replace('weather_', '')
            temp_df = pd.read_csv(file_path, parse_dates=['time'])
            temp_df['site_code'] = site_code
            
            all_weather_dfs.append(temp_df)
            print(f"successfully processed file: {file_path.name}")

        except Exception as e:
            print(f"an error occurred while processing {file_path.name}: {e}")

    if not all_weather_dfs:
        print("\nerror: no data was successfully processed from the files.")
        return

    print("\ncombining all processed files into a single dataframe...")
    combined_df = pd.concat(all_weather_dfs, ignore_index=True)

    # ensure the 'time' column is timezone-aware (utc)
    if combined_df['time'].dt.tz is None:
        combined_df['time'] = combined_df['time'].dt.tz_localize('UTC')
    else:
        combined_df['time'] = combined_df['time'].dt.tz_convert('UTC')

    combined_df.to_csv(output_path, index=False)

    print(f"full combined weather data has been saved to: {output_path}")
    print(f"final shape: {combined_df.shape}")


if __name__ == "__main__":
    prepare_full_weather_data()