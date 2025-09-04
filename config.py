from settings import settings   

RAW_JSON_BLOB         = "raw/all_crags.json"
EXTRACTED_BLOB        = "processed/crag/extracted_df.parquet"
TRANSFORMED_BLOB      = "processed/crag/transformed_df.parquet"
CRAG_PARQUET_BLOB     = "cleaned/crag/crag_df.parquet"
WEATHER_RAW_BLOB      = "processed/weather/weather_df.parquet"
WEATHER_CLEAN_BLOB    = "cleaned/weather/cleaned_weather_df.parquet"
CSV_LOAD_BLOB         = "load/weather_load.csv"

GCS_BUCKET   = settings.gcs_bucket
MAX_POINTS = settings.wx_max_points

