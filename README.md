# smart_traffic_violation_pattern_detector

# Smart Traffic Violation Analytics

## Project Overview
This project analyzes traffic violation data to identify patterns, hotspots, and trends in a city. It integrates data ingestion, cleaning, analysis, and visualization to provide actionable insights for traffic management and enforcement.

**Key Objectives:**
- Identify peak violation hours and high-risk locations.
- Detect trends in violation types and severity.
- Map traffic violation hotspots and clusters.
- Provide an interactive dashboard for exploration and reporting.

---

## Features

1. **Data Simulation**
   - Generates synthetic traffic violation data with random timestamps, locations, violation types, vehicle types, and severity levels.
   
2. **Data Ingestion**
   - Reads CSV traffic data using PySpark with a defined schema.
   - Converts raw data into Spark DataFrame for processing.
   
3. **Data Cleaning**
   - Handles missing or malformed data.
   - Validates timestamps and categorical fields.
   - Stores cleaned data in Parquet format for faster processing.
   
4. **Pattern Analysis**
   - Aggregates data by time, violation type, and location.
   - Computes hotspots using statistical methods and clustering (K-Means).
   - Produces aggregated results stored in Parquet files.
   
5. **Visualization Dashboard**
   - Streamlit-based interactive dashboard.
   - Time-based trends, violation type distribution, heatmaps, and spatial maps.
   - Export filtered datasets as CSV or JSON.

---

## Folder Structure
project_root/
│
├─ cleaned/ # Cleaned parquet files
├─ aggregated_results/ # Aggregated metrics
├─ hotspot_results/ # Hotspots and clusters
├─ DOCS/ # Project documentation
├─ schema.py # Data schema definition
├─ simulate_data.py # Synthetic data generation
├─ ingest_data.py # Data ingestion using Spark
├─ clean_data.py # Data cleaning pipeline
├─ pattern_analysis.py # Time & location-based analysis
├─ advanced_pattern_analysis.py # Hotspot detection & clustering
├─ streamlit_app.py # Interactive dashboard
└─ traffic_data.csv # Raw traffic data


---

## Technology Stack

- **Python 3.x** – Core language
- **PySpark** – Data processing
- **Pandas** – Data manipulation
- **Streamlit** – Dashboard interface
- **Plotly** – Interactive visualizations
- **Docker / Heroku (optional)** – Deployment
- **Parquet** – Optimized storage format for cleaned and aggregated data

---

## Quick Start

1. Install dependencies:
```bash
pip install -r requirements.txt

