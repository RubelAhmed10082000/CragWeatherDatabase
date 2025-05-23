import great_expectations as gx
import pandas as pd
import os

# Initialize Great Expectations context
context = gx.get_context()

# Create a new Datasource


# Load data
crag_df = pd.read_csv("crag_df.csv")
weather_df = pd.read_csv("cleaned_weather_df.csv")
name_crag = "crag_df"
name_weather = "weather_df"
data_asset = datasource.add_data_asset(name=[name_crag,name_weather], data=[crag_df,weather_df])
my_batch_request = data_asset.build_batch_request(dataframes=[crag_df, weather_df])

# Create an expectation suite 

