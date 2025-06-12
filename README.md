# London Air Pollution Forecasting Project

This project aims to build a real-time air pollution prediction system for London using historical and live hourly data from the **London Air Quality Network (LAQN)**.

We focus on modeling two major pollutants:
- **NO₂** (Nitrogen Dioxide)
- **PM2.5** (Fine Particulate Matter)

---

##  Features

-  Automatic data ingestion from LAQN API
-  Site filtering for valid, overlapping NO₂ and PM2.5 coverage
-  Supports multi-year hourly data collection
-  Modular Python scripts (Jupyter used only for prototyping)
-  Forecasting model (coming soon)
-  Visualization dashboard (planned)



---

## 📊 Data Source

Data is sourced via the London Air Quality Network (LAQN) API.

Example endpoint:
https://api.erg.ic.ac.uk/AirQuality/Data/SiteSpecies/SiteCode=MY1/SpeciesCode=NO2/StartDate=2023-01-01/EndDate=2023-01-07/Json


---

## 📌 Goals

- Enable short-term hourly air quality forecasting
- Combine air quality with traffic and weather data
- Inform urban planning and pollution control decisions

---

## 🛠 Requirements

- Python 3.9+
- pandas
- requests
- tqdm
- pyarrow (for Parquet support)
