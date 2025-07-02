import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import pyarrow.parquet as pq
import io
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@functions_framework.http
def process_taxi_data(request):
    """Cloud Function to process NYC Taxi data and load into BigQuery."""
    try:
        # Initialize clients
        logger.info("Initializing clients")
        storage_client = storage.Client()
        bq_client = bigquery.Client()

        # Define bucket and file
        bucket_name = "nyc-taxi-data-nyc-taxi-pipeline"
        source_blob_name = "raw/yellow_tripdata_2023-01.parquet"
        destination_table = "nyc_taxi_pipeline.taxi_trips"

        # Verify bucket and file exist
        logger.info(f"Accessing bucket: {bucket_name}, file: {source_blob_name}")
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        if not blob.exists():
            logger.error(f"File {source_blob_name} not found in bucket {bucket_name}")
            return f"Error: File {source_blob_name} not found in bucket {bucket_name}", 500

        # Download Parquet
        logger.info("Downloading Parquet file")
        data = blob.download_as_bytes()

        # Read Parquet in chunks with pyarrow
        logger.info("Processing Parquet in chunks")
        parquet_file = pq.ParquetFile(io.BytesIO(data))
        required_columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "passenger_count"]
        first_chunk = True

        for batch in parquet_file.iter_batches(batch_size=100000):
            logger.info(f"Processing batch of size {batch.num_rows}")
            df = batch.to_pandas()
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Missing columns in Parquet: {missing_columns}")
                return f"Error: Missing columns in Parquet: {missing_columns}", 500

            df = df[required_columns]
            df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce")
            df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"], errors="coerce")
            df = df.dropna()

            # Load to BigQuery
            logger.info(f"Loading batch to BigQuery table {destination_table}")
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE" if first_chunk else "WRITE_APPEND",
                autodetect=True
            )
            bq_client.load_table_from_dataframe(df, destination_table, job_config=job_config).result()
            first_chunk = False

        logger.info(f"Data successfully loaded to {destination_table}")
        return f"Data processed and loaded to {destination_table}"

    except Exception as e:
        logger.error(f"Error in function: {str(e)}", exc_info=True)
        return f"Error: {str(e)}", 500