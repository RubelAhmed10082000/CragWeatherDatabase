import pandas as pd
from modules.gcs_io import gcs_url, read_parquet, write_parquet


def clean_weather_data(src_blob = 'processed/weather/weather_df.parquet',
                       dst_blob = 'cleaned/weather/cleaned_weather_df.parquet'):
    """
    
    Changes column names of weather_df

    Args:
    
    weather_df (pd.DataFrame): Result of fetch_weather_data() function
    
    returns:
    
    clean_weather_df (pd.DataFrame): DataFrame with more descriptive column titles

    """

    src, dst = gcs_url(*src_blob.split("/")), gcs_url(*dst_blob.split("/"))

    try:
        weather_df = read_parquet(src)
        cleaned_weather_df = weather_df.rename(columns = {'temperature_2m':'temperature_c','relative_humidity_2m':'relative_humidity_percentage','precipitation':'precipitation_percentage'})
        cleaned_weather_df["relative_humidity_percentage"] = cleaned_weather_df["relative_humidity_percentage"].round().clip(0,100).astype("Int64")
        write_parquet(cleaned_weather_df, dst)
        print("Cleaning was successful")
        return cleaned_weather_df
    except Exception as e:
        print(f"Cleaning was unsuccessful. Error: {e}")
        return None