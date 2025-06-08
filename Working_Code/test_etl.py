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

from main import extract, transform, clean, fetch_weather_data, clean_weather_data, load


@pytest.fixture
def cleaned_weather_df():
    return pd.read_parquet('Working_Code/cleaned_weather_df.parquet')
    
@pytest.fixture
def crag_df():
    return pd.read_parquet('Working_Code/crag_df.parquet')

@pytest.fixture
def extracted_df():
    return pd.read_parquet('Working_Code/extracted_df.parquet')

@pytest.fixture
def transformed_df():
    return pd.read_parquet('Working_Code/transformed_df.parquet')

@pytest.fixture
def weather_df():
    return pd.read_parquet('Working_Code/weather_df.parquet')
 

def test_transform_output_shape_and_type(crag_df, transformed_df):
    assert isinstance(crag_df, pd.DataFrame)
    assert len(crag_df.columns) == 12
    assert len(crag_df.columns) < len(transformed_df.columns)

def test_transform_country_column(crag_df):
    assert all(crag_df['country'] == 'England')

def test_clean_column_types_crag(crag_df):
    string_columns = ['sector_name', 'crag_name', 'county', 'country', 'route_name', 'difficulty_grade']
    for col in string_columns:
        assert ptypes.is_string_dtype(crag_df[col])

    assert ptypes.is_integer_dtype(crag_df['routes_count'])
    assert isinstance(crag_df['type'].dtype, pd.CategoricalDtype)
    assert isinstance(crag_df['safety_grade'].dtype, pd.CategoricalDtype)
    assert ptypes.is_float_dtype(crag_df['latitude'])
    assert ptypes.is_float_dtype(crag_df['longitude'])

def test_clean_integrity(crag_df, transformed_df, extracted_df):
    assert isinstance(crag_df, pd.DataFrame)
    assert 'grade' not in crag_df.columns
    assert len(transformed_df.columns) > len(crag_df.columns)
    assert crag_df['routes_count'].max() == extracted_df['routes_count'].max()
    assert all(crag_df['country'] == 'England')

def test_no_nulls_crag(crag_df):
    columns = ['longitude', 'latitude']
    for col in columns:
        assert crag_df[col].notnull().all()

def test_no_nulls_weather(cleaned_weather_df):
    assert cleaned_weather_df.notnull().all().all()

def test_unique_coordinates_weather(cleaned_weather_df):
    assert (cleaned_weather_df[['longitude','latitude']].isna().sum() == 0).all()

def test_weather_clean_integrity(cleaned_weather_df, weather_df):
    assert len(cleaned_weather_df.columns) == 6
    assert len(cleaned_weather_df.columns) == len(weather_df.columns)
    assert isinstance (weather_df, pd.DataFrame)
    assert isinstance (cleaned_weather_df, pd.DataFrame)

def test_weather_columns_types(cleaned_weather_df):
    assert ptypes.is_float_dtype(cleaned_weather_df['longitude'])
    assert ptypes.is_float_dtype(cleaned_weather_df['latitude'])
    for col in ['precipitation_percentage', 'temperature_c', 'relative_humidity_percentage']:
        assert ptypes.is_float_dtype(cleaned_weather_df[col])
    #assert isinstance(cleaned_weather_df['date'].dtype, datetime64[ns])
    assert ptypes.is_datetime64_any_dtype(cleaned_weather_df['date'])

