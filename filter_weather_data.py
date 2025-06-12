import pandas as pd
from src import config as cfg

# Load clean weather data (UTC time)
input_path  = cfg.RAW_WTH / "weather_data.csv"
output_path = cfg.INT_WTH / "weather_filtered.parquet"
cfg.INT_WTH.mkdir(parents=True, exist_ok=True)

print(f"Loading: {input_path}")
df = pd.read_csv(input_path)

# Convert UTC time to London time
df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
df['time'] = df['time'].dt.tz_convert('Europe/London')

# Filter: Weekdays (Mon–Fri), Hours (7–18), Months (Mar–Oct)
is_weekday = df['time'].dt.dayofweek.between(0, 4)
is_hour    = df['time'].dt.hour.between(7, 18)
is_month   = df['time'].dt.month.between(3, 10)

filtered_df = df[is_weekday & is_hour & is_month].copy()

print(f"Records before filtering: {len(df):,}")
print(f"Records after filtering : {len(filtered_df):,}")

# Save
filtered_df.to_parquet(output_path, index=False)
print(f"Filtered weather data saved to:\n{output_path}")
