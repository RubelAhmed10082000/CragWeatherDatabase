from modules.extract import extract
from modules.transform import transform
from modules.clean import clean
from modules.fetch_weather_data import fetch_weather_data
from modules.clean_weather_data import clean_weather_data
from modules.load import load_from_parquet
from modules.validate import validate


extracted_df = extract('data/raw/all_crags.json')
transformed_df = transform(extracted_df)
crag_df = clean(transformed_df)
weather_df = fetch_weather_data(crag_df)
cleaned_weather_df = clean_weather_data(weather_df)
print("crag_df shape:", crag_df.shape)
print("cleaned_weather_df shape:", cleaned_weather_df.shape)
print("crag_df cols:", list(crag_df.columns))
print("weather cols:", list(cleaned_weather_df.columns))
load_from_parquet(
    "data/processed/crag_df.parquet",
    "data/processed/cleaned_weather_df.parquet"
)
