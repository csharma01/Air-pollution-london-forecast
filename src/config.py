from pathlib import Path

# repo root = one directory up from *this* file’s parent
ROOT  = Path(__file__).resolve().parents[1]          # …/Air-pollution-london
DATA  = ROOT / "data"

RAW        = DATA / "raw"
RAW_TRF    = RAW / "traffic"
RAW_AQ     = RAW / "pollution"
RAW_WTH    = RAW / "weather"

INTERIM    = DATA / "interim"
INT_TRF    = INTERIM / "traffic"
INT_AQ     = INTERIM / "pollution"
INT_WTH = INTERIM / "weather"

FINAL      = DATA / "final"
FIN_TRF    = FINAL / "traffic"
FIN_MERGED = FINAL / "aq_traffic"

SITES_POLLUTION = ROOT / "src" / "data" / "selected_sites.csv"
MATCHED_SITES = ROOT / "data" / "matched_sites_laqn_to_dft.csv"