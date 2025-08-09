import requests_cache
from retry_requests import retry
import openmeteo_requests
import pandas as pd
from time import sleep, time


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
        weather_df.to_parquet('data/processed/weather_df.parquet', index=None)
        print(f"API Call was successful.{weather_df.shape}")
        return weather_df

    except Exception as e:
        print(f"There was an error: {e}")
    
    time.sleep(0.15)
