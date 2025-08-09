import pandas as pd

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
        cleaned_weather_df.to_parquet('data/processed/cleaned_weather_df.parquet', index=None)
        print("Cleaning was successful")
        return cleaned_weather_df
    except Exception as e:
        print(f"Cleaning was unsuccessful. Error: {e}")
        return None