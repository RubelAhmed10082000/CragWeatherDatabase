import os
from modules.gcs_io import gcs_url
from modules.extract import extract
from modules.transform import transform
from modules.clean import clean as clean_crag
from modules.fetch_weather_data import fetch_weather_data
from modules.clean_weather_data import clean_weather_data
from modules.load import load_from_gcs

# Setting up GCS file pathways 

RAW_JSON = gcs_url("raw", "all_crags.json")

EXTRACTED_PARQUET   = gcs_url("processed", "crag", "extracted_df.parquet")
TRANSFORMED_PARQUET = gcs_url("processed", "crag", "transformed_df.parquet")
CRAG_PARQUET        = gcs_url("processed", "crag", "crag_df.parquet")

WEATHER_RAW_PARQUET = gcs_url("processed", "weather", "weather_df.parquet")
WEATHER_CLEAN_PARQ  = gcs_url("cleaned", "weather", "cleaned_weather_df.parquet")

CSV_ARCHIVE_PREFIX  = "archive/csv" 

MAX_POINTS = int(os.getenv("WX_MAX_POINTS", "50"))

def main():

    # Checking if enviroment variable exist
    if not os.getenv("DATABASE_URL"):
        raise RuntimeError("DATABASE_URL not set")
    if not os.getenv("GCS_BUCKET"):
        raise RuntimeError("GCS_BUCKET not set (used by gcs_url)")
    
    df_ex = extract(json_blob_name="raw/all_crags.json") 

    transform(
        src_blob="processed/crag/extracted_df.parquet",
        dst_blob="processed/crag/transformed_df.parquet",
    )

    clean_crag(
        src_blob="processed/crag/transformed_df.parquet",
        dst_blob="cleaned/crag/crag_df.parquet",
    )

    fetch_weather_data(
        crag_src="cleaned/crag/crag_df.parquet",
        dst="processed/weather/weather_df.parquet",
        max_points=MAX_POINTS,  
    )

    clean_weather_data(
        src_blob="processed/weather/weather_df.parquet",
        dst_blob="cleaned/weather/cleaned_weather_df.parquet",
    )

    clean_weather_data(
        src_blob="processed/weather/weather_df.parquet",
        dst_blob="cleaned/weather/cleaned_weather_df.parquet",
    )

    load_from_gcs (
    crag_parquet_gs    = gcs_url("cleaned","crag","crag_df.parquet"),
    weather_parquet_gs = gcs_url("cleaned","weather","cleaned_weather_df.parquet"),
    csv_archive_prefix = "archive/csv"   
)

if __name__ == "__main__":
    main()


