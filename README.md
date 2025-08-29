#  London Air Pollution Forecasting Project

This project develops **machine learning models** to forecast **hourly NO₂ (Nitrogen Dioxide)** and **PM2.5 (Fine Particulate Matter)** concentrations across selected London monitoring sites.  
It integrates multi-source datasets — air quality, traffic, and meteorology — and applies ensemble models (**Random Forest** and **LightGBM**) for short-term prediction.

The models aim to support **urban planning, transport policy, and public health alerts** by providing practical, data-driven insights into air quality fluctuations.

---

##  Data Sources

- **London Air Quality Network (LAQN)** – Hourly pollutant concentrations (NO₂, PM2.5).  
  [LAQN API](https://www.londonair.org.uk/london/asp/datadownload.asp)  

- **UK Department for Transport (DfT)** – Annual Average Daily Flow (AADF) traffic counts.  
  [DfT Traffic Data](https://roadtraffic.dft.gov.uk/downloads)  

- **Open-Meteo API** – Hourly weather variables (wind, temperature, humidity, etc.).  
  [Open-Meteo](https://open-meteo.com/)  

---

##  Tech Stack & Libraries

- **Languages**: Python 3.9+  
- **Libraries**:  
  - Data processing: `pandas`, `numpy`, `holidays`  
  - Machine learning: `scikit-learn`, `lightgbm`  
  - Visualisation: `matplotlib`, `seaborn`  
  - File formats: `pyarrow` (for Parquet)  
  - Utilities: `tqdm`, `requests`  

---
## Project Structure
```
Air-pollution-london/
│
├── data/ # All datasets 
│ ├── raw/ # Original datasets (NOT INCLUDED IN GITHUB, SEE LINKS FOR DATA SOURCES BELOW)
│ │ ├── pollution/ # LAQN pollution data
│ │ ├── traffic/ # DfT traffic counts
│ │ └── weather/ # Site-level weather data
│ ├── interim/ # Cleaned intermediate outputs
│ │ ├── pollution/
│ │ ├── traffic/
│ │ └── weather/
│ └── final/aq_traffic/ # Final model-ready dataset
│
├── src/ # Source files
│ ├── config.py # Centralised configuration
│ └── eda/ # Exploratory data analysis notebooks
│
├── scripts/ # Python scripts for data pipeline
| ├── Random_forest.ipynb
│ ├── LightGBM.ipynb
│ ├── fetch_data_laqn.py
│ ├── filter_laqn_data.py
│ ├── fetch_weather_data.py
│ ├── prepare_weather_data.py
│ ├── match_sites_AQ_traffic.py
│ └── build_model_dataset.py
│
├── README.md # Project overview
└── .gitignore
```
---

## Pipeline Overview  

The project workflow is modular, with each stage producing outputs for the next:  

1. **Fetch Air Quality Data**  
   ```
   python fetch_data_laqn.py
   ```
   - Downloads hourly **NO₂** and **PM2.5 (plus FINE backup)** from LAQN API (2010–2025) for selected sites.  
   - Saves to `data/raw/pollution/london_air_quality_2010_2025.csv`.  

2. **Clean & Impute Air Quality Data**  
   ```
   python filter_laqn_data.py
   ```
   - Pivots to wide format with site metadata.  
   - Combines reference PM2.5 with non-reference FINE.  
   - Uses **KNNImputer (k=5)** for gaps.  
   - Outputs `data/interim/pollution/laqn_wide.csv`.  

3. **Match AQ Sites to Traffic Counters**  
   ```
   python match_sites_AQ_traffic.py
   ```
   - Matches each LAQN site to nearest DfT AADF traffic counter using Haversine distance.  
   - Outputs `data/matched_sites_laqn_to_dft.csv`.  

4. **Fetch Weather Data**  
   ```
   python fetch_weather_data.py
   ```
   - Downloads site-level hourly weather (Open-Meteo archive).  
   - Outputs per-site CSVs in `data/raw/weather/`.  

5. **Prepare Combined Weather Data**  
   ```
   python prepare_weather_data.py
   ```
   - Merges site weather into one file.  
   - Ensures UTC timezone consistency.  
   - Outputs `data/interim/weather/weather_combined_full.csv`.  

6. **Build Final Modelling Dataset**  
   ```
   python build_model_dataset.py
   ```
   - Merges AQ, weather, and DfT traffic data.  
   - Engineers features:  
     - **Temporal**: month, day, hour, weekend, holiday, rush hour  
     - **Policy**: ULEZ (Apr 2019), COVID lockdown (Mar 2020–Dec 2021)  
     - **Lagged pollutants**: 12h, 24h  
     - **Traffic & road type**  
   - Validates no missing values.  
   - Saves final dataset as `data/final/aq_traffic/model_ready_dataset.parquet`.  

7. **Train & Evaluate Models**  
   - Run Jupyter notebooks:  
     - `Random_forest.ipynb`  
     - `LightGBM.ipynb`  
   - Includes **TimeSeriesSplit cross-validation**, hyperparameter tuning, feature importance plots, and evaluation metrics (**R², MAE**).  

---

##  Key Results (Summary)

| Site Group               | Model        | NO₂ R² | NO₂ MAE (µg/m³) | PM2.5 R² | PM2.5 MAE (µg/m³) |
|---------------------------|-------------|--------|-----------------|----------|-------------------|
| Bexley & Greenwich (suburban) | Random Forest | 0.555  | ~6.5            | 0.514    | ~4.0              |
| Camden & Hackney (urban)      | Random Forest | 0.542  | ~8.5            | 0.375    | ~3.8              |
| Bexley & Greenwich            | LightGBM      | 0.637  | ~6.4            | 0.559    | ~3.6              |
| Camden & Hackney              | LightGBM      | 0.620  | ~7.9            | 0.419    | ~3.0              |

- **LightGBM outperformed Random Forest**, particularly for PM2.5.  
- Suburban sites were easier to predict than central London.  
- Lagged pollutant values, traffic volume, and meteorology were key drivers.  

---

## How to Run  

1. **Clone the repository**  
   ```
   git clone https://github.com/csharma01/Air-pollution-london-forecast.git
   cd Air-pollution-london-forecast
   ```

2. **Set up environment**  
   ```
   pip install -r requirements.txt
   ```

3. **Run pipeline scripts in sequence**  
   ```
   python fetch_data_laqn.py
   python filter_laqn_data.py
   python match_sites_AQ_traffic.py
   python fetch_weather_data.py
   python prepare_weather_data.py
   python build_model_dataset.py
   ```

4. **Experiment with models**  
   Launch Jupyter:  
   ```
   jupyter lab
   ```
   Then run notebooks:  
   - `Random_forest.ipynb`  
   - `LightGBM.ipynb`  

---

##  Future Work

- Incorporate **regional background pollution** (transboundary PM2.5).  
- Experiment with **deep learning architectures** (LSTM, CNN-LSTM hybrids).  
- Develop a **real-time forecasting dashboard**.  

---
