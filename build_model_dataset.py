import pandas as pd
import numpy as np
import holidays
from src import config as cfg

def run_final_build():

    try:
        aq_df = pd.read_csv(cfg.INT_AQ / 'laqn_wide.csv', parse_dates=['date'])
        weather_df = pd.read_csv(cfg.INT_WTH / "weather_combined_full.csv")
        traffic_map = pd.read_csv(cfg.MATCHED_SITES)
        traffic_counts = pd.read_csv(cfg.RAW_TRF / "dft_traffic_counts_aadf.csv", low_memory=False)
        print("source files loaded successfully.")
    except FileNotFoundError as e:
        print(f"critical error: missing source file\n{e}")
        return

    # prepare and merge air quality and weather data
    aq_df.rename(columns={'NO2_final': 'NO2', 'PM2.5_final': 'PM2.5'}, inplace=True)
    aq_df['date'] = pd.to_datetime(aq_df['date']).dt.tz_localize('UTC')
    weather_df.rename(columns={'time': 'date'}, inplace=True)
    weather_df['date'] = pd.to_datetime(weather_df['date'], utc=True)
    df = pd.merge(aq_df, weather_df, on=['date', 'site_code'], how='inner')
    print("air quality and weather data merged.")

    # prepare and merge yearly traffic data
    traffic_agg = traffic_counts.groupby(['count_point_id', 'year'])['all_motor_vehicles'].mean().reset_index()
    traffic_map_cols = ['site_code', 'nearest_count_point_id', 'road_type']
    df['year'] = df['date'].dt.year
    df = pd.merge(df, traffic_map[traffic_map_cols], on='site_code', how='left')
    df = pd.merge(df, traffic_agg, left_on=['nearest_count_point_id', 'year'], right_on=['count_point_id', 'year'], how='left')
    df.rename(columns={'all_motor_vehicles': 'aadf_vehicle_count'}, inplace=True)
    df.sort_values(by=['site_code', 'date'], inplace=True)
    df['aadf_vehicle_count'] = df.groupby('site_code')['aadf_vehicle_count'].ffill().bfill()
    df['aadf_vehicle_count'].fillna(df['aadf_vehicle_count'].mean(), inplace=True)
    print("traffic data and road type merged and imputed.")

    # feature engineering
    df['date_local'] = df['date'].dt.tz_convert('Europe/London')
    df['month'] = df['date_local'].dt.month
    df['day_of_week'] = df['date_local'].dt.dayofweek
    df['hour'] = df['date_local'].dt.hour
    
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
    uk_holidays = holidays.UK(subdiv='ENG', years=range(2010, 2026))
    df['is_holiday'] = df['date_local'].dt.date.isin(uk_holidays).astype(int)
    
    is_weekday = ~df['is_weekend'].astype(bool)
    morning_rush = (df['hour'] >= 6) & (df['hour'] <= 9)
    evening_rush = (df['hour'] >= 16) & (df['hour'] <= 21)
    df['is_rush_hour'] = np.where(is_weekday & (morning_rush | evening_rush), 1, 0)
    
    df = pd.get_dummies(df, columns=['road_type'], prefix='road')
    
    # finalize and save
    cols_to_drop = ['nearest_count_point_id', 'count_point_id', 'date_local', 'SiteID']
    df.drop(columns=cols_to_drop, inplace=True, errors='ignore')

    # final validation checks
    assert df.isnull().sum().sum() == 0, "error: missing values found in the final dataset."
    print("validation checks passed.")
    
    output_path = cfg.FIN_MERGED / "model_ready_dataset.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    
    print(f"\nfinal dataset saved to {output_path}")
    print(f"final shape: {df.shape}")
    print("\nsample of final columns:", df.columns.tolist())

if __name__ == "__main__":
    run_final_build()