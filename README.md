# 🌦️ GHCN-D Climate Analytics ETL Pipeline

A modular, PySpark-based ETL pipeline to ingest, transform, and analyze daily climate observations using the GHCN-D dataset. Built with Databricks Notebooks and Delta Lake in the **Medallion Architecture** (Bronze → Silver → Gold), this pipeline processes NOAA climate data for **Georgia (GA), USA**, from **2015–2025**, generating ML-ready features and climate analytics.

## Project Structure

```
GHCN-D ETL Project/
├── config/                        # YAML-based configuration
│   └── pipeline_config.yaml
├── notebooks/                     # Orchestrated Databricks Notebooks
│   ├── 01_data_ingestion.py
│   ├── 02_bronze_processing.py
│   ├── 03_silver_processing.py
│   ├── 04_gold_processing.py
│   └── 05_data_validation.py
├── src/                           # Modular PySpark logic
│   ├── ingest/
│   │   ├── data_downloader.py
│   │   ├── file_extractor.py
│   ├── transform/
│   │   ├── bronze_processor.py
│   │   ├── silver_processor.py
│   │   ├── gold_processor.py
│   └── utils/
│       ├── config_loader.py
│       ├── data_validator.py
│       ├── spark_utils.py
│       └── schema_definitions.py
└── README.md
```

## Database Structure

```
workspace/
└── ghcn/
    └── ghcnvol/
        └── data/
            ├── raw
            ├── bronze
            ├── silver
            └── gold
```

## Quickstart

### Setup

- **Databricks Cluster**: Use a Free Tier or Serverless compute cluster.
- **Unity Volume**: Create if not already:
  ```
  /Volumes/workspace/ghcn/ghcnvol/
  ```

### Upload Files

- Upload the pipeline_config.yaml to:
  ```
  /Workspace/Users//GHCN-D ETL Project/config/
  ```
- Upload source files (`src/`) and notebooks (`notebooks/`).

### Run Orchestration Notebook

> Run all pipeline notebooks sequentially:

```python
dbutils.notebook.run("01_data_ingestion.py", 3600)
dbutils.notebook.run("02_bronze_processing.py", 3600)
dbutils.notebook.run("03_silver_processing.py", 3600)
dbutils.notebook.run("04_gold_processing.py", 3600)
dbutils.notebook.run("05_data_validation.py", 3600)
```

## Data Sources

| Dataset            | Description                            | Source (HTTPS)                                                   |
| ------------------ | -------------------------------------- | ---------------------------------------------------------------- |
| ghcnd-stations.txt | Station metadata                       | https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt |
| ghcnd_all.tar.gz   | All daily climate records (.dly files) | https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd_all.tar.gz   |

> Filters only `STATE == "GA"` and `2015 ≤ year ≤ 2025`.

## Medallion Architecture

| Layer  | Description                                                       |
| ------ | ----------------------------------------------------------------- |
| Bronze | Raw GHCN `.dly` files parsed into structured format               |
| Silver | Cleaned records with station metadata join + unit normalization   |
| Gold   | Monthly/yearly aggregates, feature engineering, ML-ready datasets |

## Data Quality & Validation

The pipeline includes:

- **Schema validation** (via `schema_definitions.py`)
- **Temperature & precipitation range checks**
- **Anomaly flagging & quality scoring (0.0 – 1.0)**
- **Data completeness metrics** by element
- **Validation notebook**: `05_data_validation.py`

## Machine Learning Readiness

The gold layer exposes **ML-ready features**, including:

- Rolling 7-day windows (avg/min/max)
- Lag values (1-day)
- Temperature range, precipitation sums
- Seasonal encodings (month sin/cos)
- Anomaly detection (deviation from monthly normals)

## Example Use Cases

- **Climate Analytics**: Trends, extremes, precipitation regimes
- **Weather Forecasting**: Feature inputs for ML models
- **Agricultural Data Science**: Freeze, heat, and rain risk modeling
- **Geo-spatial Dashboards**: Join with geolocation tools or APIs

## Configuration

Edit `config/pipeline_config.yaml` to control pipeline behavior:

```yaml
data_sources:
  base_url: "https://www.ncei.noaa.gov/pub/data/ghcn/daily/"
  stations_file: "ghcnd-stations.txt"
  daily_data_file: "ghcnd_all.tar.gz"

processing:
  target_state: "GA"
  start_year: 2015
  end_year: 2025
  required_elements: ["TMAX", "TMIN", "PRCP", "SNOW", "SNWD"]

storage:
  raw_path: "/Volumes/workspace/ghcn/ghcnvol/data/raw"
  bronze_path: "/Volumes/workspace/ghcn/ghcnvol/data/bronze"
  silver_path: "/Volumes/workspace/ghcn/ghcnvol/data/silver"
  gold_path: "/Volumes/workspace/ghcn/ghcnvol/data/gold"
```

## Serverless Compatibility

> This project works seamlessly on **Databricks Serverless Compute**.

✅ Supports `mapInPandas()` and native dataframe APIs  
⚠️ Avoids `RDD` or `sc` usage (not allowed in serverless mode)  
✅ Compatible with Unity Catalog + Volumes

## Completed Output

- ✅ Delta tables for raw, cleaned & aggregated data
- ✅ Validated climate records and metadata
- ✅ Aggregates: Monthly, yearly, and climate normals
- ✅ Parquet exports of ML-ready feature tables
