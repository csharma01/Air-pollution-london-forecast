import pandas as pd
import numpy as np
from src import config as cfg
from math import radians, cos, sin, asin, sqrt

def vectorized_haversine(lat1, lon1, lat2, lon2):
    R = 6371
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1.reshape(-1, 1)
    dlon = lon2 - lon1.reshape(-1, 1)
    a = np.sin(dlat / 2)**2 + np.cos(lat1).reshape(-1, 1) * np.cos(lat2) * np.sin(dlon / 2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c

def main():
    # load data
    laqn_sites = pd.read_csv(cfg.INT_AQ / 'laqn_wide.csv')[['site_code', 'site_name', 'latitude', 'longitude']].drop_duplicates('site_code').reset_index(drop=True)
    dft_df = pd.read_csv(cfg.RAW_TRF / "dft_traffic_counts_aadf.csv", low_memory=False)
    dft_sites = dft_df[['count_point_id', 'latitude', 'longitude', 'road_type']].dropna().drop_duplicates('count_point_id').reset_index(drop=True)

    # calculate distances and find nearest
    distance_matrix = vectorized_haversine(laqn_sites['latitude'].values, laqn_sites['longitude'].values, dft_sites['latitude'].values, dft_sites['longitude'].values)
    nearest_dft_indices = np.argmin(distance_matrix, axis=1)
    min_distances = np.min(distance_matrix, axis=1)
    nearest_dft_sites = dft_sites.loc[nearest_dft_indices]

    # construct final dataframe
    matches_df = pd.DataFrame({
        'site_code': laqn_sites['site_code'],
        'site_name': laqn_sites['site_name'],
        'laqn_lat': laqn_sites['latitude'],
        'laqn_lon': laqn_sites['longitude'],
        'nearest_count_point_id': nearest_dft_sites['count_point_id'].values,
        'dft_lat': nearest_dft_sites['latitude'].values,
        'dft_lon': nearest_dft_sites['longitude'].values,
        'road_type': nearest_dft_sites['road_type'].values,
        'distance_km': min_distances
    })
    
    # save the mapping file
    output_path = cfg.MATCHED_SITES
    output_path.parent.mkdir(parents=True, exist_ok=True)
    matches_df.to_csv(output_path, index=False)
    
    print(f"mapping complete. new file saved to: {output_path}")

if __name__ == "__main__":
    main()