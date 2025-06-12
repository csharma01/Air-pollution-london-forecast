# src/prep/reshape_laqn.py
import pandas as pd
from src import config as cfg

# Load raw LAQN data
laqn_path = cfg.RAW_AQ / "laqn_2010_2025.parquet"
df = pd.read_parquet(laqn_path)

# Standardize timestamp
df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

# Pivot pollutant_name into columns: NO2, PM2.5
wide_df = (
    df.pivot_table(index=["site_code", "timestamp"],
                   columns="pollutant_name",
                   values="value")
      .reset_index()
)

# Remove multi-level column name
wide_df.columns.name = None

# Rename pollutant columns
wide_df = wide_df.rename(columns={
    "NO2": "no2",
    "PM2.5": "pm25"
})

# Save output
out_path = cfg.INT_AQ / "laqn_wide.parquet"
wide_df.to_parquet(out_path, index=False)

# Print confirmation and summary
print(f"LAQN reshaped and saved to: {out_path} | shape: {wide_df.shape}")

# Summary statistics
print("\nSummary Statistics (after reshaping):")
for col in ["no2", "pm25"]:
    print(f"\n {col.upper()}")
    print("  Missing values:", wide_df[col].isna().sum())
    print("  Min           :", wide_df[col].min())
    print("  Max           :", wide_df[col].max())
    print("  Mean          :", round(wide_df[col].mean(), 2))
