import pandas as pd
import json

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
        extracted_df.to_parquet('data/processed/extracted_df.parquet', index=None)
        print("file successfully turned into a dataframe")
        return extracted_df
    except Exception as E:
        print(f"file was not successfully turned into dataframe. Error: {E}")
        return None