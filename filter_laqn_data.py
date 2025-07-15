import pandas as pd
from sklearn.impute import KNNImputer
from src import config as cfg

input_file = cfg.RAW_AQ / "london_air_quality_2010_2025.csv"
df = pd.read_csv(input_file)

# Convert timestamp to datetime objects
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Pivot the data, keeping metadata
df_pivoted = df.pivot_table(
    index=['timestamp', 'site_code', 'site_name', 'latitude', 'longitude'],
    columns='pollutant',
    values='value'
).reset_index()

# Rename the timestamp column for clarity
df_pivoted = df_pivoted.rename(columns={"timestamp": "date"})

# Set the date as the main index for time-series operations
df_pivoted = df_pivoted.set_index('date')

# Group by Site
# Create a dictionary of DataFrames, one for each site
sites = {site_code: group for site_code, group in df_pivoted.groupby('site_code')}

# Clean and Impute All Sites
print(f"Starting imputation for {len(sites)} sites...")
final_imputed_sites = {}
imputer = KNNImputer(n_neighbors=5)

for site_code, site_df in sites.items():
    df_copy = site_df.copy()

    # Combine reference PM2.5 and non-reference FINE data
    df_copy['pm25_combined'] = df_copy['PM2.5'].combine_first(df_copy['FINE'])

    # Use KNN Imputation for any remaining gaps in NO2 and the combined PM2.5
    impute_cols = ['NO2', 'pm25_combined']
    df_copy[impute_cols] = imputer.fit_transform(df_copy[impute_cols])

    # Rename final columns
    df_copy = df_copy.rename(columns={'NO2': 'NO2_final', 'pm25_combined': 'PM2.5_final'})

    # Keep only the necessary final columns along with metadata
    final_cols = ['site_code', 'site_name', 'latitude', 'longitude', 'NO2_final', 'PM2.5_final']
    final_imputed_sites[site_code] = df_copy[final_cols]


# Combine and Save Final Dataset
final_dataset = pd.concat(final_imputed_sites.values())

output_path = cfg.INT_AQ / 'full_clean_air_quality_final.csv'

output_path.parent.mkdir(parents=True, exist_ok=True)

final_dataset.to_csv(output_path)

print(f"\nFinal dataset saved to: {output_path}")