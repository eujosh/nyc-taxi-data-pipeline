# NYC Taxi Data Pipeline on Google Cloud Platform

## Overview
This project builds an automated, serverless data pipeline on GCP to process NYC Taxi Trip data. It ingests raw CSV data, processes it with Cloud Functions, stores it in BigQuery, and automates execution with Cloud Scheduler. A sample analysis shows average trip distances by hour.

## Architecture
- **Data Source**: NYC Taxi Trip Data (January 2023, public dataset).
- **Storage**: Google Cloud Storage for raw and processed data.
- **Processing**: Cloud Functions for data cleaning and transformation.
- **Data Warehouse**: BigQuery for storage and querying.
- **Orchestration**: Cloud Scheduler for automation.

## Setup Instructions
1. Set up a GCP project with free trial credits.
2. Upload `yellow_tripdata_2023-01.csv` to a Cloud Storage bucket.
3. Deploy the Cloud Function (see `cloud_function/main.py`).
4. Create a BigQuery dataset (`nyc_taxi_pipeline`).
5. Schedule the pipeline with Cloud Scheduler.
6. Run SQL queries in BigQuery for analysis.

## Sample Analysis
- Query: Average trip distance by hour of the day.
- Results stored in `nyc_taxi_pipeline.trip_summary`.

## Tools Used
- Google Cloud Platform (Cloud Storage, Cloud Functions, BigQuery, Cloud Scheduler)
- Python, Pandas, SQL

## Files
- `cloud_function/main.py`: Cloud Function code to process data.
- `cloud_function/requirements.txt`: Python dependencies.
- `README.md`: Project documentation.

## Screenshots
- [Add paths to screenshots, e.g., `screenshots/bucket.png`]

## Learnings
- Built a serverless ETL pipeline on GCP.
- Managed costs within the GCP Free Trial.
- Used BigQuery for large-scale data analysis.
