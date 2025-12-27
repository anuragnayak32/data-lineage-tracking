## Data Provenance and Lineage Tracking using FastAPI, SQLite, and Pandas.

## Overview
This project implements a data provenance and lineage tracking system that records the complete history of data transformations across an ETL pipeline. It captures where data originates from, what operations are applied to it, and when each transformation occurs. The system enables transparent, auditable, and reproducible data workflows, which are critical in real-world analytics and machine learning pipelines.

---

## Problem Statement
In data engineering and machine learning systems, datasets undergo multiple transformations such as cleaning, filtering, and feature engineering. Without proper lineage tracking, it becomes difficult to understand how a dataset was produced, debug data issues, or reproduce ML experiments. This project addresses that problem by maintaining a structured lineage log and exposing it through a REST API.

---

## Key Features
- Tracks data source, transformation steps, timestamps, and metadata  
- Supports multiple datasets and dataset versions using unique dataset IDs  
- Stores lineage information in a JSON-based ledger backed by SQLite  
- Exposes REST APIs to query lineage history for any dataset  
- Clean separation between ETL logic and API service  

---

## Tech Stack
- **Python 3**
- **Pandas** – data processing and ETL
- **SQLite** – lightweight lineage storage
- **FastAPI** – REST API framework
- **JSON** – structured lineage metadata

---

## Project Structure

```text
data-lineage-tracking/
├── app/
│   ├── __init__.py
│   ├── main.py
│   ├── routes.py
│   ├── models.py
│   └── database.py
│
├── etl/
│   └── etl_pipeline.py
│
├── data/
│   └── raw_sales.csv
│
├── db/
│   └── lineage.db
│
├── requirements.txt
├── README.md
└── .gitignore
```
 
---

## How It Works
1. The ETL script reads raw data using Pandas.
2. Each transformation step (read, clean, type casting, versioning) is logged with metadata.
3. Lineage records are stored in a SQLite database as JSON.
4. A FastAPI service exposes an endpoint to retrieve lineage history by dataset ID.
5. Swagger UI is used to interactively test the API.

---

## API Endpoint
---

## Running the Project

### Install dependencies
```bash
pip install -r requirements.txt
```

### Start FastAPI server
```bash
python -m uvicorn app.main:app
```

### Run ETL:
```bash
python etl/etl_pipeline.py
```

### Open Swagger UI
```bash
http://127.0.0.1:8000/docs
```



