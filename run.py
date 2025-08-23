import os
from modules.gcs_io import gcs_url
from modules.extract import extract
from modules.transform import transform
from modules.clean import clean as clean_crag
from modules.fetch_weather_data import fetch_weather_data
from modules.clean_weather_data import clean_weather_data
from modules.load import load_weather_snapshot_from_gcs

from config import (
    RAW_JSON_BLOB,
    EXTRACTED_BLOB,
    TRANSFORMED_BLOB,
    CRAG_PARQUET_BLOB,
    WEATHER_RAW_BLOB,
    WEATHER_CLEAN_BLOB,
    CSV_LOAD_BLOB,
    MAX_POINTS,
)

def main():
    if not os.getenv("DATABASE_URL"):
        raise RuntimeError("DATABASE_URL not set")
    if not os.getenv("GCS_BUCKET"):
        raise RuntimeError("GCS_BUCKET not set")

    extract(json_blob_name=RAW_JSON_BLOB)

    transform(src_blob=EXTRACTED_BLOB, dst_blob=TRANSFORMED_BLOB)

    clean_crag(src_blob=TRANSFORMED_BLOB, dst_blob=CRAG_PARQUET_BLOB)

    fetch_weather_data(
        crag_src=CRAG_PARQUET_BLOB,         
        dst=WEATHER_RAW_BLOB,             
        max_points=MAX_POINTS,
    )

    clean_weather_data(
        src_blob=WEATHER_RAW_BLOB,          
        dst_blob=WEATHER_CLEAN_BLOB,        
    )

    load_weather_snapshot_from_gcs(
        weather_parquet_gs=gcs_url(*WEATHER_CLEAN_BLOB.split("/")),
        csv_gs_uri=gcs_url(*CSV_LOAD_BLOB.split("/")),
    )

if __name__ == "__main__":
    main()
