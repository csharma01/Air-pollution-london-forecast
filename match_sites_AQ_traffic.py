import pandas as pd
import numpy as np
from pathlib import Path
from math import radians, cos, sin, asin, sqrt
import sys
from src import config as cfg


# Haversine formula to calculate distance between two lat/lon points (in km)
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in kilometers
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 2 * R * asin(sqrt(a))


def main():
    # Load LAQN site metadata
    laqn = pd.read_csv(cfg.SITES_POLLUTION)
    laqn = laqn[["site_code", "site_name", "latitude", "longitude"]].dropna()

    # Load DfT traffic site metadata
    dft = pd.read_csv(cfg.RAW_TRF / "dft_rawcount_region_id_6.csv", low_memory=False)
    dft = dft[["count_point_id", "latitude", "longitude"]].dropna().drop_duplicates("count_point_id")

    matches = []

    for _, laqn_row in laqn.iterrows():
        site_code = laqn_row["site_code"]
        lat1 = laqn_row["latitude"]
        lon1 = laqn_row["longitude"]

        # Calculate distance to all DfT points
        dft["distance_km"] = dft.apply(
            lambda row: haversine(lat1, lon1, row["latitude"], row["longitude"]), axis=1
        )
        nearest = dft.loc[dft["distance_km"].idxmin()]

        matches.append({
            "site_code": site_code,
            "site_name": laqn_row["site_name"],
            "laqn_lat": lat1,
            "laqn_lon": lon1,
            "nearest_count_point_id": nearest["count_point_id"],
            "dft_lat": nearest["latitude"],
            "dft_lon": nearest["longitude"],
            "distance_km": nearest["distance_km"]
        })

    # Save mapping result
    out_df = pd.DataFrame(matches)
    out_df.to_csv(cfg.DATA / "matched_sites_laqn_to_dft.csv", index=False)
    print("Matching completed. Saved to: matched_sites_laqn_to_dft.csv")


if __name__ == "__main__":
    main()
