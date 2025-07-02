import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    df = pd.read_parquet("yellow_tripdata_2023-01.parquet", engine="pyarrow")
    logger.info("Parquet read successfully")
    required_columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "passenger_count"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"Missing columns: {missing_columns}")
    else:
        df = df[required_columns]
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce")
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"], errors="coerce")
        df = df.dropna()
        logger.info(f"Processed {len(df)} rows")
except Exception as e:
    logger.error(f"Error: {str(e)}", exc_info=True)