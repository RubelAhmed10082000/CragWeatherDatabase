import sys

sys.path.append('..')

from modules.extract import extract
from modules.transform import transform
from modules.clean import clean
from modules.fetch_weather_data import fetch_weather_data
from modules.clean_weather_data import clean_weather_data
from modules.load import load
from modules.validate import validate


extracted_df = extract('data/raw/all_crags.json')
transformed_df = transform(extracted_df)
crag_df = clean(transformed_df)
weather_df = fetch_weather_data(crag_df)
cleaned_weather_df = clean_weather_data(weather_df)
validate()
load(crag_df, cleaned_weather_df)
