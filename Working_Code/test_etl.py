import json
import pandas as pd
import ast
import os
import numpy as np
import logging
import pytest
import pandas.api.types as ptypes
import logging
import openmeteo_requests
from retry_requests import retry
import requests_cache

from main.py import extract, transform, clean, fetch_weather_data, clean_weather_data

extracted_df = extract('all_crags.json')
transformed_df = transform(extracted_df)
crag_df = clean(transformed_df)
weather_df = fetch_weather_data(crag_df)
cleaned_weather_df = clean_weather_data(weather_df)



for attempt in range(0, 3):
    print(f"attempt: {attempt}")
    extracted_df = extract('all_crags.json')
    transformed_df = transform(extracted_df)
    crag_df = clean(transformed_df)
    weather_df = fetch_weather_data(crag_df)  # Pass crag_df to fetch_weather_data
    cleaned_weather_df = clean_weather_data(weather_df)
    load(crag_df, cleaned_weather_df)   
    if attempt == 2:
        print("Test successful")


def test_transform_output_shape_and_type():
    assert isinstance(transformed_df, pd.DataFrame)
    assert len(transformed_df.columns) == 17
    assert len(extracted_df.columns) < len(transformed_df.columns)

def test_transform_country_column():
    assert all(transformed_df['country'] == 'England')

def test_clean_column_types_crag(crag_df):
    string_columns = ['sector_name', 'crag_name', 'county', 'country', 'route_name', 'coordinates', 'difficulty_grade']
    for col in string_columns:
        assert ptypes.is_string_dtype(crag_df[col])

    assert ptypes.is_int64_dtype(crag_df['routes_count'])
    assert ptypes.is_categorical_dtype(crag_df['type'])

def test_clean_integrity():
    assert isinstance(crag_df, pd.DataFrame)
    assert len(crag_df.columns) == 13
    assert 'grade' not in crag_df.columns
    assert len(transformed_df.columns) > len(crag_df.columns)
    assert crag_df['routes_count'].max() == transformed_df['routes_count'].max()
    assert all(crag_df['country'] == 'England')

def test_no_nulls_crag():
    columns = ['crag_name','route_name','county','country','coordinates']
    for col in columns:
        assert crag_df[col].notnull().all()

def test_unique_columns_crag():
    assert crag_df['route_name'].is_unique

def test_no_nulls_weather(cleaned_weather_df):
    assert cleaned_weather_df.notnull().all()

def test_unique_coordinates_weather():
    assert cleaned_weather_df[['longitude','latitude']].is_unqiue

def test_weather_clean_integrity():
    assert len(cleaned_weather_df.columns) == 6
    assert len(cleaned_weather_df.columns) == len(weather_df.columns)
    assert isinstance (weather_df, pd.DataFrame)
    assert isinstance (cleaned_weather_df, pd.DataFrame)

def test_weather_columns_types():
    assert ptypes.is_float_dtype(cleaned_weather_df[['longitude','latitude']])
    assert ptypes.is_float_dtype(cleaned_weather_df[[x]])
    assert ptypes.is_datetime64_dtype(cleaned_weather_df[['date']])

def log_weather_pipeline():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Starting weather data pipeline")

    try:
        extracted_df = extract('all_crags.json')
        transformed_df = transform(extracted_df)
        crag_df = clean(transformed_df)
        weather_df = fetch_weather_data(crag_df)
        cleaned_weather_df = clean_weather_data(weather_df)

        if cleaned_weather_df is not None:
            logging.info("Successfully extracted, transformed and cleaned weather data")
        else:
            logging.warning("Pipeline completed but final data is None — check earlier steps")

    except Exception as e:
        logging.error(f"Pipeline failed with error: {e}")
