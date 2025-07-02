# C:\Air pollution london\build_model_dataset.py

import pandas as pd
import numpy as np
import holidays
import re
from src import config as cfg


def clean_col_names(col_name):
    """cleans weather column names by removing units and making them lowercase."""
    new_name = re.sub(r'\s*\([^)]*\)', '', col_name)
    new_name = new_name.replace(' ', '_').replace('/', '_')
    return new_name.lower()


def run_final_build():

    # load datasets
    try:
        aq_df = pd.read_parquet(cfg.INT_AQ / "laqn_wide.parquet")
        weather_df = pd.read_parquet(cfg.INT_WTH / "weather_combined_full.parquet")
        matched_sites_df = pd.read_csv(cfg.MATCHED_SITES)
        aadf_df = pd.read_csv(cfg.RAW_TRF / "dft_traffic_counts_aadf.csv", low_memory=False)
        print("source files loaded successfully.")
    except FileNotFoundError as e:
        print(f"critical error: missing source file\n{e}")
        return

    # prepare and merge data
    aq_df.rename(columns={'timestamp': 'date', 'site_code': 'SiteID', 'no2': 'NO2', 'pm25': 'PM2.5'}, inplace=True)
    aq_df['date'] = pd.to_datetime(aq_df['date'], utc=True)

    weather_df.rename(columns={'time': 'date'}, inplace=True)
    weather_df['date'] = pd.to_datetime(weather_df['date'], utc=True)
    weather_df.columns = [clean_col_names(col) for col in weather_df.columns]
    weather_df.rename(columns={'siteid': 'SiteID'}, inplace=True)

    df = pd.merge(aq_df, weather_df, on=['date', 'SiteID'], how='inner')
    print("core datasets merged.")

    # prepare and merge traffic data
    aadf_agg = aadf_df.groupby(['count_point_id', 'year'])['all_motor_vehicles'].mean().reset_index()
    traffic_map = matched_sites_df[['site_code', 'nearest_count_point_id']].rename(columns={'site_code': 'SiteID'})

    df['year'] = df['date'].dt.year
    df = pd.merge(df, traffic_map, on='SiteID', how='left')
    df = pd.merge(df, aadf_agg, left_on=['nearest_count_point_id', 'year'], right_on=['count_point_id', 'year'],
                  how='left')
    df.rename(columns={'all_motor_vehicles': 'aadf_vehicle_count'}, inplace=True)

    df.sort_values(by=['SiteID', 'date'], inplace=True)

    # use a two-step fill to handle all missing aadf values
    df['aadf_vehicle_count'] = df.groupby('SiteID')['aadf_vehicle_count'].ffill().bfill()
    df['aadf_vehicle_count'].fillna(df['aadf_vehicle_count'].mean(), inplace=True)

    # create time-based features
    df['date'] = pd.to_datetime(df['date']).dt.tz_convert('Europe/London')
    df['hour'] = df['date'].dt.hour
    df['day_of_week'] = df['date'].dt.dayofweek
    df['month'] = df['date'].dt.month
    df['day_of_year'] = df['date'].dt.dayofyear

    # create cyclical and categorical features
    df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24.0)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24.0)
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12.0)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12.0)
    df['dow_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7.0)
    df['dow_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7.0)
    df['doy_sin'] = np.sin(2 * np.pi * df['day_of_year'] / 365.25)
    df['doy_cos'] = np.cos(2 * np.pi * df['day_of_year'] / 365.25)

    df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
    is_weekday = ~df['is_weekend'].astype(bool)
    morning_rush = (df['hour'] >= 7) & (df['hour'] <= 9)
    evening_rush = (df['hour'] >= 16) & (df['hour'] <= 19)
    df['is_rush_hour'] = np.where(is_weekday & (morning_rush | evening_rush), 1, 0)

    uk_holidays = holidays.UK(subdiv='ENG', years=range(2010, 2026))
    df['is_holiday'] = df['date'].dt.date.isin(uk_holidays).astype(int)

    # create cyclical and categorical weather features
    if 'wind_direction_10m' in df.columns:
        df['wind_dir_sin'] = np.sin(df['wind_direction_10m'] * (np.pi / 180))
        df['wind_dir_cos'] = np.cos(df['wind_direction_10m'] * (np.pi / 180))

    if 'weather_code' in df.columns:
        df = pd.get_dummies(df, columns=['weather_code'], prefix='wc', dummy_na=True)

    # finalize dataset by dropping columns
    cols_to_drop = ['nearest_count_point_id', 'count_point_id', 'wind_direction_10m']
    df.drop(columns=cols_to_drop, inplace=True, errors='ignore')

    # final validation checks
    print("validating final dataset...")
    assert df['aadf_vehicle_count'].isna().sum() == 0, "error: missing traffic data after imputation."
    assert df.duplicated(subset=['date', 'SiteID']).sum() == 0, "error: duplicate rows found."
    print("validation checks passed.")

    # save final dataset
    output_path = cfg.FIN_MERGED / "model_ready_dataset.parquet"
    df.to_parquet(output_path, index=False)
    print(f"\nsuccess: final dataset saved to {output_path}")
    print(f"final shape: {df.shape}")


if __name__ == "__main__":
    run_final_build()