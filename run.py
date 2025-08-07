from src.extract import extract
from src.transform import transform
from src.clean import clean
from src.fetch_weather_data import fetch_weather_data
from src.clean_weather_data import clean_weather_data
from src.load import load
from src.validate import validate

extracted_df = extract('data/raw/all_crags.json')
transformed_df = transform(extracted_df)
crag_df = clean(transformed_df)
weather_df = fetch_weather_data(crag_df)
cleaned_weather_df = clean_weather_data(weather_df)
validate()
load(crag_df, cleaned_weather_df)
