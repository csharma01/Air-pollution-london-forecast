# C:\Air pollution london\prepare_weather_data.py

import pandas as pd
import re
from src import config as cfg


def clean_col_names(col_name):
    """cleans column names by removing units, special characters, and making them lowercase."""
    new_name = re.sub(r'\s*\([^)]*\)', '', col_name)
    new_name = new_name.replace(' ', '_').replace('/', '_')
    return new_name.lower()


def prepare_full_weather_data():

    input_dir = cfg.RAW_WTH
    output_dir = cfg.INT_WTH
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "weather_combined_full.parquet"

    weather_files = list(input_dir.glob("weather_*.csv"))

    if not weather_files:
        print(f"error: no weather files were found in {input_dir}")
        return

    print(f"found {len(weather_files)} weather files to combine.")

    all_weather_dfs = []
    for file_path in weather_files:
        try:
            site_code = file_path.stem.split('_')[1]
            temp_df = pd.read_csv(file_path, low_memory=False)

            # standardize column names immediately after loading
            temp_df.columns = [clean_col_names(col) for col in temp_df.columns]

            temp_df['siteid'] = site_code

            # convert all columns to numeric except for 'time' and 'siteid'
            cols_to_convert = [col for col in temp_df.columns if col not in ['time', 'siteid']]
            for col in cols_to_convert:
                temp_df[col] = pd.to_numeric(temp_df[col], errors='coerce')

            temp_df.dropna(inplace=True)

            all_weather_dfs.append(temp_df)
            print(f"successfully processed file: {file_path.name}")

        except Exception as e:
            print(f"an error occurred while processing {file_path.name}: {e}")

    if not all_weather_dfs:
        print("\nerror: no data was successfully processed from the files.")
        return

    print("\ncombining all processed files into a single dataframe...")
    combined_df = pd.concat(all_weather_dfs, ignore_index=True)

    # convert the 'time' column to a proper timezone-aware datetime object
    combined_df['time'] = pd.to_datetime(combined_df['time'], utc=True)

    combined_df.to_parquet(output_path, index=False)

    print("\nsuccess! the process is complete.")
    print(f"full combined weather data has been saved to: {output_path}")
    print(f"\nfinal shape: {combined_df.shape}")


if __name__ == "__main__":
    prepare_full_weather_data()