import json
import pandas as pd
import ast
import os
import numpy as np
import openmeteo_requests
from retry_requests import retry
import time
import requests_cache
import duckdb
from datetime import datetime 
from time import sleep
from expectations import run_expectations

def extract(raw_data):
    """
    Turns raw data into datafame

    Args: 
    raw_data (.json): Raw data file (Thanks Ricardo!)
    
    Returns: 
    pd.DataFrame: Extracted data 
    
    """
    # Opening file via context manager
    try:
        with open(raw_data) as f:
            all_crags = json.load(f)
            print("file successfully opened")
    except Exception as E:
        print(f"Something went wrong. Error: {E}")
        return None
    # Normalizing the JSON data into a DataFrame
    try:
        extracted_df = pd.json_normalize(all_crags, record_path=['crags'])
        extracted_df.to_parquet('dags/Files/extracted_df.parquet', index=None)
        print("file successfully turned into a dataframe")
        return extracted_df
    except Exception as E:
        print(f"file was not successfully turned into dataframe. Error: {E}")
        return None


def transform(extracted_data):
    """
    Normalizes dataframe and explodes columns
        
    Args: 
    
    extracted_data (pd.DataFrame): Extracted data. Result of extract() function
    
    Returns: 
    
    Transformed Data (pd.DataFrame): The transformed function with columns exploded and normalised
    
    """
    # Check if extracted_data is None
    if extracted_data is None:
        print("No data to transform")
        return None

    try:
        transformed_df = extracted_data.explode('routes.sectors').reset_index(drop=True)
        
        # Normalize sectors
        sectors_df = pd.json_normalize(transformed_df['routes.sectors'])
        
        # Attach original crag columns
        for col in transformed_df.columns:
            if col != 'routes.sectors':
                sectors_df[col] = transformed_df[col].values
        
        # Explode routes
        sectors_df = sectors_df.explode('routes').reset_index(drop=True)
        
        # Now normalize the routes
        routes_df = pd.json_normalize(sectors_df['routes'])
        
        # Rename route name early to avoid conflict
        routes_df = routes_df.rename(columns={'name': 'route_name'})
        
        # Drop the now redundant 'routes' column
        sectors_df = sectors_df.drop(columns=['routes'])
        
        # Join route info with crag+sector info
        transformed_df = pd.concat([sectors_df.reset_index(drop=True), routes_df.reset_index(drop=True)], axis=1)
        
        # Rename crag-related columns
        transformed_df = transformed_df.rename(columns={'name': 'crag_name', 'id': 'crag_id'})
    
        transformed_df.to_parquet('dags/Files/transformed_df.parquet', index=None)
        print(f"file successfully normalized. Dataframe has {transformed_df.shape}")
        return transformed_df
        
    except Exception as e:
        print(f"Transformation unsuccessful: {e}")
        return None


def clean(transformed_data):
    """
    
    Cleans transformed dataframe. Produces new columns, applies appropriate data types, drops unneeded columns and applies np.nan

    Args:
    
    transformed_data (pd.DataFrame): The transformed data. Result of transform() function
    
    Returns: 
    
    cleaned_data (pd.DataFrame): Dataframe that has been cleaned
    
    """
    if transformed_data is None:
        print("No data to clean")
        return None
    
    try:
        # Dropping unnecessary columns
        crag_df = transformed_data.drop(columns=['direction', 'is_hill', 'slug', 'difficulty', 'stars'])
        
        # Changing columns to relevant data types
        astype_crag = {'crag_name': 'string', 'county': 'string', 'country': 'string', 'rocktype': 'category', 'sector_name': 'string', 'grade': 'string', 'type': 'category', 'longitude': 'float64', 'latitude': 'float64', 'route_name': 'string'}
        
        crag_df = crag_df.astype(astype_crag)
        
        # Removing any rows where the longitude and latitude are 0
        crag_df = crag_df.loc[~((crag_df['longitude'] == 0) | (crag_df['latitude'] == 0))]
        
        # Replacing 'Summit' or 'summit' with NaN
        crag_df = crag_df.replace(['Summit', 'summit'], np.nan)

        # Applying np.nan to blank cells in relevant columns
        crag_df = crag_df.fillna(value=np.nan)

        # Replacing all nulls in sector_name with 'Main Area'
        crag_df['sector_name'] = crag_df['sector_name'].replace(np.nan, 'Main Area')
       
        # Creating both 'difficulty_grade' and 'safety_grade' columns
        crag_df['difficulty_grade'] = crag_df['grade'].apply(lambda x: x.split(' ', 1)[1] if isinstance(x, str) and ' ' in x else x).astype('string')
        crag_df['safety_grade'] = crag_df['grade'].apply(lambda x: x.split(' ', 1)[0] if isinstance(x, str) and ' ' in x else np.nan).astype('string')
        
        # Dropping grade column
        crag_df = crag_df.drop(columns=['grade'])
        
        # Setting ID as index
        crag_df = crag_df.set_index('crag_id')

        crag_df[['longitude','latitude']] = crag_df[['longitude','latitude']].dropna()
        
        # Exporting function result to .csv
        crag_df.to_parquet('dags/Files/crag_df.parquet', index=None)
        print(f"file successfully cleaned. Dataframe has {crag_df.shape}")
        return crag_df
    except Exception as e:
        print(f"Cleaning unsuccessful: {e}")
        return None
    
def fetch_weather_data(crag_df):
    """
    Calls Open-Meteo API to create weather_df
    
    Args: cleaned_data (pd.DataFrame) (in this case crag_df). Needs to be an argument as longitude and latitude are from this dataframe will be passed as arguments
    
    Result:

    Weather_df (DataFrame): 7 Day weather forecast data

    """
    try:
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)
    
        # Assuming crag_df is defined somewhere in the notebook
        unique_coords = crag_df[['latitude', 'longitude']].dropna().drop_duplicates().head(50)
   
        # Prepare list to hold weather results
        weather_results = []
    
        
        # Step 3: Loop through each coordinate
        for _, row in unique_coords.iterrows():
            lat = float(row['latitude'])
            lon = float(row['longitude'])
    
            # Make sure all required weather variables are listed here
            # The order of variables in hourly or daily is important to assign them correctly below
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": lat,
                "longitude": lon,
                "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation"],
                "wind_speed_unit": "mph"
            }
            responses = openmeteo.weather_api(url, params=params)
            
            # Process first location. Add a for-loop for multiple locations or weather models
            response = responses[0]
            print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
            print(f"Elevation {response.Elevation()} m asl")
            print(f"Timezone {response.Timezone()}{response.TimezoneAbbreviation()}")
            print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")
            
            # Process hourly data. The order of variables needs to be the same as requested.
            hourly = response.Hourly()
            hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
            hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
            hourly_precipitation = hourly.Variables(2).ValuesAsNumpy()
            
            # Create a dictionary to hold the hourly data
            hourly_data = {"date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            )}
            
            # Add the weather variables to the dictionary
            hourly_data["temperature_2m"] = hourly_temperature_2m
            hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
            hourly_data["precipitation"] = hourly_precipitation
            
            # Convert to DataFrame and add latitude and longitude
            df = pd.DataFrame(hourly_data)
            df["latitude"] = lat
            df["longitude"] = lon
    
            weather_results.append(df)
            sleep(0.15)

        # Concatenate all weather dataframes into one
        weather_df = pd.concat(weather_results).reset_index(drop=True)
        weather_df.to_parquet('dags/Files/weather_df.parquet', index=None)
        print(f"API Call was successful.{weather_df.shape}")
        return weather_df

    except Exception as e:
        print(f"There was an error: {e}")
    
    time.sleep(0.15)

def clean_weather_data(weather_df):
    """
    
    Changes column names of weather_df

    Args:
    
    weather_df (pd.DataFrame): Result of fetch_weather_data() function
    
    returns:
    
    clean_weather_df (pd.DataFrame): DataFrame with more descriptive column titles

    """
    try:
        cleaned_weather_df = weather_df.rename(columns = {'temperature_2m':'temperature_c','relative_humidity_2m':'relative_humidity_percentage','precipitation':'precipitation_percentage'})
        cleaned_weather_df.to_parquet('dags\Files\cleaned_weather_df.parquet', index=None)
        print("Cleaning was successful")
        return cleaned_weather_df
    except Exception as e:
        print(f"Cleaning was unsuccessful. Error: {e}")
        return None
    
def load (crag_df, cleaned_weather_df):
    """
    Loads both dataframes into DuckDB and creates a simple OLAP Database.

    Args:

    crag_df (pd.DataFrame) = crag dataframe
    cleaned_weather_database (pd.Dataframe) = weather dataframe

    Returns: SQL database
    """
    try:
        con = duckdb.connect("file.db")
        con.register('crag_df', crag_df)
        con.register('cleaned_weather_df', cleaned_weather_df)
        con.sql('''
            DROP TABLE IF EXISTS fact_hourlyrouteweather;
            DROP TABLE IF EXISTS dimRoutes;
            DROP TABLE IF EXISTS dimHourlyWeatherInfo;
            DROP TYPE IF EXISTS rocktype;
            DROP TYPE IF EXISTS type;

            CREATE TYPE type AS ENUM (
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
            'Via Ferrata'
            );
                
        
            CREATE TYPE rocktype AS ENUM (
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
            'Psammite'
            );
            
            
            CREATE TABLE dimHourlyWeatherInfo (
                weather_id int,
                date TIMESTAMP,
                precipitation_percentage int,
                temperature_c FLOAT,
                longitude FLOAT,
                latitude FLOAT,
                relative_humidity_percentage int,
                PRIMARY KEY (weather_id)
            );
        
            INSERT INTO dimHourlyWeatherInfo (weather_id, date, precipitation_percentage, temperature_c, longitude, latitude, relative_humidity_percentage)
            SELECT 
                ROW_NUMBER() OVER ()::int, 
                date, 
                precipitation_percentage, 
                temperature_c, 
                longitude, 
                latitude, 
                relative_humidity_percentage
            FROM cleaned_weather_df;

            CREATE TABLE dimRoutes (
                route_id int,
                route_name VARCHAR,
                climbing_type type,
                safety_grade VARCHAR,
                climbing_grade VARCHAR,
                sector_name VARCHAR,
                rocktype rocktype,
                longitude FLOAT,
                latitude FLOAT,
                route_count int,
                country VARCHAR,
                county VARCHAR,
                PRIMARY KEY (route_id)
            );
        
            INSERT INTO dimRoutes (
                route_id,
                route_name,
                climbing_type,
                safety_grade,
                climbing_grade,
                sector_name,
                rocktype,
                longitude,
                latitude,
                route_count,
                country,
                county
            )
            SELECT 
                ROW_NUMBER() OVER ()::int, 
                route_name, 
                type, 
                safety_grade, 
                difficulty_grade, 
                sector_name, 
                rocktype, 
                longitude, 
                latitude, 
                routes_count, 
                country, 
                county
            FROM crag_df;
        
            CREATE TABLE fact_hourlyrouteweather (
                route_id INTEGER REFERENCES dimRoutes (route_id),
                weather_id INTEGER REFERENCES dimHourlyWeatherInfo (weather_id),
                date TIMESTAMP,
                relative_humidity_percentage INTEGER,
                temperature_c FLOAT,
                precipitation_percentage INTEGER 
            );
        
            INSERT INTO fact_hourlyrouteweather (
                route_id,
                weather_id,
                date,
                relative_humidity_percentage,
                temperature_c,
                precipitation_percentage
            )
            SELECT 
                dimRoutes.route_id,
                dimHourlyWeatherInfo.weather_id,
                dimHourlyWeatherInfo.date,
                dimHourlyWeatherInfo.relative_humidity_percentage,
                dimHourlyWeatherInfo.temperature_c,
                dimHourlyWeatherInfo.precipitation_percentage
            FROM dimHourlyWeatherInfo 
            JOIN dimRoutes 
            ON ROUND(dimHourlyWeatherInfo.latitude,4) = ROUND(dimRoutes.latitude,4) AND ROUND(dimHourlyWeatherInfo.longitude,4) = ROUND(dimRoutes.longitude,4);
        ''')
        result = con.sql("SELECT * FROM fact_hourlyrouteweather LIMIT 5").fetchdf()

        print("\n Tables in the DuckDB database:")
        print(con.sql("SHOW TABLES").fetchdf())

        
        print("\n Schema of fact_hourlyrouteweather:")
        print(con.sql("DESCRIBE fact_hourlyrouteweather").fetchdf())
        
        con.close()
        print ("Data sucessfully loaded to DuckDB")
        return result
    
    except Exception as e:
        con.close()
        print (f"Something went wrong with the loading, error:{e}")
        return None
    
    finally:
        con.close()
        print("Connection to DuckDB closed.")   




