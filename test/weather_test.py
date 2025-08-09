import pandas as pd
import pytest
import pandas.api.types as ptypes

@pytest.fixture
def weather_df():
    return pd.read_parquet('data/processed/cleaned_weather_df.parquet')

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


def test_no_duplicates_weather(cleaned_weather_df):
    assert not cleaned_weather_df.duplicated().any()



# Expect this test to fail as main.py currently fetches only weather data for the first 50 crags, being more conveinent for testing.
@pytest.mark.xfail(reason="Weather data currently only fetched for first 50 crags")
def test_weather_df_row_count(cleaned_weather_df):
    assert len(cleaned_weather_df) == 712992, f"Expected 712992 rows, but got {len(cleaned_weather_df)}"



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


def test_weather_values_reasonable(cleaned_weather_df):
    assert cleaned_weather_df['temperature_c'].between(-20, 45).all()
    assert cleaned_weather_df['relative_humidity_percentage'].between(0, 100).all()
    assert cleaned_weather_df['precipitation_percentage'].between(0, 100).all()

def test_weather_coordinates_exist_in_crags(cleaned_weather_df, crag_df):
    weather_coords = cleaned_weather_df[['latitude', 'longitude']].drop_duplicates()
    crag_coords = crag_df[['latitude', 'longitude']].drop_duplicates()
    merged = weather_coords.merge(crag_coords, on=['latitude', 'longitude'], how='left')
    assert merged.notnull().all().all()


