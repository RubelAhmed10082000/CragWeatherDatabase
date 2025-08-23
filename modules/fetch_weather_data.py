import requests_cache
from retry_requests import retry
import openmeteo_requests
import pandas as pd
from time import sleep, time
from modules.gcs_io import gcs_url, read_parquet, write_parquet



def fetch_weather_data(crag_src = 'cleaned/crag/crag_df.parquet', dst = 'processed/weather/weather_df.parquet',
                       max_points = 50):
    """
    Calls Open-Meteo API to create weather_df 
    Exports to gcs storage bucket as parquet file
    
    Args: 
    
    Cleaned_data (pd.DataFrame) (in this case crag_df). 
    Needs to be an argument as longitude and latitude are from this dataframe will be passed as arguments

    max_points(int) How many rows of the crag_df the API will call,
    for testing purposes as you don't want to call for all 4,000 crags when testing

    Result:

    Weather_df (DataFrame): 7 Day weather forecast data.
    Includes: [date, percipitation_percentage, temperature_c, longitude, latitude, relative_humidity_percentage]

    """
    try:
        crag_df = read_parquet(gcs_url(*crag_src.split("/")))
    except FileNotFoundError:
        print(f"Source not found {crag_src}")
        return None
    
    if crag_df is None or crag_df.empty:
        print(f"No Crag data to fetch for")
        return None
    
    if not {"latitude","longitude"}.issubset(crag_df.columns):
        print("crag_df missing latitude/longitude columns")
        return None

    coords = crag_df[["latitude", "longitude"]].dropna().drop_duplicates()
    unique_coords = coords.head(max_points) if max_points else coords

    
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

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
            "hourly": ["temperature_2m", "relative_humidity_2m","precipitation_probability"],
            "wind_speed_unit": "mph"
        }

        try:

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

            for col in ("relative_humidity_2m",):
                df[col] = df[col].round().clip(0, 100).astype("Int64")

            weather_results.append(df) 

        except Exception as e:
            print (f"Failed for ({lat}, {lon}: {e})")

        sleep(0.15)

    # Concatenate all weather dataframes into one
    weather_df = pd.concat(weather_results).reset_index(drop=True)
    write_parquet(weather_df, gcs_url(*dst.split("/")))
    print(f"API Call was successful.{weather_df.shape}")
    return weather_df

    
