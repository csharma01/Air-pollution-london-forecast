# C:\Air pollution london\build_model_dataset.py

import pandas as pd
import numpy as np
import holidays
import re
from src import config


def clean_col_names(col_name):
    """Cleans weather column names by removing units and making them lowercase."""
    new_name = re.sub(r'\s*\([^)]*\)', '', col_name)
    new_name = new_name.replace(' ', '_').replace('/', '_')
    return new_name.lower()


def run_final_build():
    """
    Executes the final data build pipeline using pre-processed files to create
    the master dataset for modeling.
    """
    print("--- Starting Final Data Build Pipeline ---")

    # === STEP 1: LOAD ALL PRE-PROCESSED AND SUPPORTING DATA ===
    print("\n[1/4] Loading pre-processed and supporting files...")
    try:
        # Load the clean, intermediate files you created
        aq_df = pd.read_parquet(config.INT_AQ / "laqn_wide.parquet")
        weather_df = pd.read_parquet(config.INT_WTH / "weather_filtered.parquet")

        # Load the supporting files for mapping and feature data
        matched_sites_df = pd.read_csv(config.MATCHED_SITES)
        aadf_df = pd.read_csv(config.RAW_TRF / "dft_traffic_counts_aadf.csv", low_memory=False)
        print("Source files loaded successfully.")
    except FileNotFoundError as e:
        print(f"Error: Could not find a source file. Please ensure all preliminary scripts have been run.\n{e}")
        return

    # === STEP 2: PREPARE AND MERGE CORE DATASETS ===
    print("\n[2/4] Merging Air Quality and pre-filtered Weather data...")

    # Prepare AQ data: standardize column names and data types
    aq_df.rename(columns={'timestamp': 'date', 'site_code': 'SiteID', 'no2': 'NO2', 'pm25': 'PM2.5'}, inplace=True)
    aq_df['date'] = pd.to_datetime(aq_df['date'], utc=True)

    # Prepare Weather data: standardize column names and data types
    # NOTE: Your weather data is already filtered by time, which simplifies this step.
    weather_df.rename(columns={'time': 'date'}, inplace=True)
    weather_df['date'] = pd.to_datetime(weather_df['date'], utc=True)
    weather_df.columns = [clean_col_names(col) for col in weather_df.columns]
    weather_df.rename(columns={'siteid': 'SiteID'}, inplace=True)

    # Merge the two core datasets on both date and site identifier
    df = pd.merge(aq_df, weather_df, on=['date', 'SiteID'], how='inner')
    print("Core datasets merged.")

    # === STEP 3: ENGINEER FINAL FEATURES (TRAFFIC & TIME) ===
    print("\n[3/4] Engineering final features...")

    # Merge the nearest_count_point_id from your mapping file to link to AADF data
    site_map = matched_sites_df[['site_code', 'nearest_count_point_id']].rename(columns={'site_code': 'SiteID'})
    df = pd.merge(df, site_map, on='SiteID', how='left')

    # Engineer time-based features using London time
    df.set_index('date', inplace=True)
    df.index = df.index.tz_convert('Europe/London')

    df['hour'] = df.index.hour
    df['day_of_week'] = df.index.dayofweek
    df['month'] = df.index.month
    df['year'] = df.index.year
    df['is_rush_hour'] = np.where(df['hour'].isin([7, 8, 9, 16, 17, 18]), 1, 0)
    uk_holidays = holidays.UK(subdiv='ENG', years=df.index.year.unique())
    df['is_holiday'] = df.index.normalize().isin(uk_holidays).astype(int)

    # Prepare and merge the AADF data as a location-based feature
    aadf_subset = aadf_df[['year', 'count_point_id', 'all_motor_vehicles']].copy()
    aadf_subset.rename(columns={'all_motor_vehicles': 'aadf_vehicle_count', 'count_point_id': 'nearest_count_point_id'},
                       inplace=True)

    df.reset_index(inplace=True)
    df = pd.merge(df, aadf_subset, on=['year', 'nearest_count_point_id'], how='left')
    print("Features engineered and merged.")

    # === STEP 4: FINALIZE AND SAVE ===
    print("\n[4/4] Finalizing and saving the master dataset...")
    # Clean up intermediate columns used for merging
    df.drop(columns=['nearest_count_point_id'], inplace=True, errors='ignore')

    if df.empty:
        print("\nError: The final pipeline resulted in an empty dataframe. Please check the merge keys.")
    else:
        # Save the final output to the 'final/aq_traffic' directory
        output_dir = config.FIN_MERGED
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "model_ready_dataset.parquet"

        df.to_parquet(output_path, index=False)
        print(f"\nSuccess! The final model-ready dataset has been saved to: {output_path}")
        print("\nFinal Data Preview:")
        print(df.head().to_string())


if __name__ == "__main__":
    run_final_build()