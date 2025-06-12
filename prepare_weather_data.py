# C:\Air pollution london\prepare_weather.py

import pandas as pd
from src import config as cfg


def prepare_weather_data():
    """
    Finds all individual weather CSV files, combines them, cleans the data,
    filters the combined data to the project's specific timeframe, and saves
    the result as a single, clean Parquet file.
    """
    print("--- Starting Full Weather Preparation Script ---")

    # Define input and output paths from your config
    input_dir = cfg.RAW_WTH
    output_dir = cfg.INT_WTH
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "weather_filtered.parquet"

    # --- Step 1: Find and Combine all raw weather files ---
    weather_files = list(input_dir.glob("weather_*.csv"))

    if not weather_files:
        print(f"Error: No weather files were found in {input_dir}")
        print("Please ensure your files are named like 'weather_WM0.csv'.")
        return

    print(f"Found {len(weather_files)} weather files to combine.")

    all_weather_dfs = []
    for file_path in weather_files:
        try:
            site_code = file_path.stem.split('_')[1]
            temp_df = pd.read_csv(file_path, skiprows=3, low_memory=False)

            # Clean numeric columns and remove bad rows (footers)
            numeric_cols = [col for col in temp_df.columns if
                            '°C' in col or '(mm)' in col or '(km/h)' in col or '(%)' in col]
            for col in numeric_cols:
                temp_df[col] = pd.to_numeric(temp_df[col], errors='coerce')
            temp_df.dropna(inplace=True)

            # Add the SiteID column for mapping
            temp_df['SiteID'] = site_code
            all_weather_dfs.append(temp_df)

        except Exception as e:
            print(f"An error occurred while processing {file_path.name}: {e}")

    if not all_weather_dfs:
        print("Error: No data was successfully processed from the files.")
        return

    print("\nCombining all raw files into a single DataFrame...")
    combined_df = pd.concat(all_weather_dfs, ignore_index=True)
    print(f"Records before filtering: {len(combined_df):,}")

    # --- Step 2: Filter the Combined DataFrame ---
    print("Applying time-based filters (Mon-Fri, 7-18h, Mar-Oct)...")

    # Convert the 'time' column to a proper datetime object (in London time)
    # The raw data is ISO format, which pandas reads as an object/string
    combined_df['time'] = pd.to_datetime(combined_df['time'], utc=True).dt.tz_convert('Europe/London')

    # Apply filters
    is_weekday = combined_df['time'].dt.dayofweek.between(0, 4)
    is_hour = combined_df['time'].dt.hour.between(7, 18)
    is_month = combined_df['time'].dt.month.between(3, 10)

    filtered_df = combined_df[is_weekday & is_hour & is_month].copy()
    print(f"Records after filtering: {len(filtered_df):,}")

    # --- Step 3: Save the Final Filtered File ---
    filtered_df.to_parquet(output_path, index=False)

    print("\nSuccess! The process is complete.")
    print(f"Filtered weather data has been saved to:\n{output_path}")
    print(f"\nFinal shape: {filtered_df.shape}")


if __name__ == "__main__":
    prepare_weather_data()