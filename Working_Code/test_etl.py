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

def test_crag_name_survived(crag_df):
    assert 'Clints Crag (Wainwrights summit)' in crag_df['crag_name'].values

def test_route_name_survived(crag_df):
    assert 'The Nose' in crag_df['route_name'].values

def test_rocktype_category(crag_df):
    expected_categories = (
            'Gritstone',
            'Limestone',
            'Sandstone (hard)',
            'Granite',
            'Grit (quarried)',
            'Sandstone (soft)',
            'Rhyolite',
            'UNKNOWN',
            'Artificial',
            'Culm',
            'Slate',
            'Greenstone',
            'Volcanic tuff',
            'Dolerite',
            'Andesite',
            'Gabbro',
            'Killas slate',
            'Mica schist',
            'Shale',
            'Pillow lava',
            'Conglomerate',
            'Chalk',
            'Schist',
            'Amphibiolite & S',
            'Welded Tuff',
            'Quartzite',
            'Crumbly rubbish',
            'Hornstone',
            'Basalt',
            'Diorites',
            'Welsh igneous',
            'Ice',
            'Serpentine',
            'Iron Rock',
            'Ignimbrite',
            'Microgranite',
            'Psammite',
            'UNKNOWN'
            )
    actual_categories = set(crag_df['rocktype'].cat.categories)
    assert actual_categories == set(expected_categories), f"Expected categories: {expected_categories}, but got: {actual_categories}"

def test_type_category(crag_df):
    expected_categories = (
            'Bouldering',
            'Trad',
            'Sport',
            'Top Rope',
            'Winter',
            'DWS',
            'Scrambling',
            'Mixed',
            'Boulder Circuit',
            'Aid',
            'Ice',
            'Alpine',
            'Via Ferrata',
            )
    actual_categories = set(crag_df['type'].cat.categories)
    assert actual_categories == set(expected_categories), f"Expected categories: {expected_categories}, but got: {actual_categories}"

def test_no_duplicates_weather(cleaned_weather_df):
    assert not cleaned_weather_df.duplicated().any()

def test_crag_df_row_count(crag_df):
    assert len(crag_df) == 138754, f"Expected 138754 rows, but got {len(cleaned_weather_df)}"

def test_weather_df_row_count(cleaned_weather_df):
    assert len(cleaned_weather_df) == 8400, f"Expected 8400 rows, but got {len(cleaned_weather_df)}"

def test_crag_column_present(crag_df):
    expected_columns = [
        'sector_name', 'crag_name', 'county', 'country', 'rocktype',
        'latitude', 'longitude', 'routes_count', 'route_name', 'type',
        'difficulty_grade', 'safety_grade'
    ]
    for col in expected_columns:
        assert col in crag_df.columns, f"Column {col} is missing from crag_df"

def test_weather_column_present(cleaned_weather_df):
    expected_columns = [
        'date', 'temperature_c', 'relative_humidity_percentage',
        'precipitation_percentage', 'longitude', 'latitude'
    ]
    for col in expected_columns:
        assert col in cleaned_weather_df.columns, f"Column {col} is missing from cleaned_weather_df"

def test_coordinates_validity(cleaned_weather_df):
    assert cleaned_weather_df['longitude'].between(-180, 180).all(), "Longitude values are out of bounds"
    assert cleaned_weather_df['latitude'].between(-90, 90).all(), "Latitude values are out of bounds"

def test_route_count_reasonable(crag_df):
    assert crag_df['routes_count'].min() >= 0 
    assert crag_df['routes_count'].max() <= 1500

def test_weather_values_reasonable(cleaned_weather_df):
    assert cleaned_weather_df['temperature_c'].between(-20, 45).all()
    assert cleaned_weather_df['relative_humidity_percentage'].between(0, 100).all()
    assert cleaned_weather_df['precipitation_percentage'].between(0, 100).all()

def test_weather_coordinates_exist_in_crags(cleaned_weather_df, crag_df):
    weather_coords = cleaned_weather_df[['latitude', 'longitude']].drop_duplicates()
    crag_coords = crag_df[['latitude', 'longitude']].drop_duplicates()
    merged = weather_coords.merge(crag_coords, on=['latitude', 'longitude'], how='left')
    assert merged.notnull().all().all()

def test_end_to_end_pipeline(tmp_path):
    extracted_df = extract('all_crags.json')
    transformed_df = transform(extracted_df)
    crag_df = clean(transformed_df)
    weather_df = fetch_weather_data(crag_df)
    cleaned_weather_df = clean_weather_data(weather_df)
    load(crag_df, cleaned_weather_df, tmp_path)
