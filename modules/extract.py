import pandas as pd
import json
from modules.gcs_io import gcs_url, write_parquet
import fsspec

def extract(json_blob_name = 'raw/all_crags.json'):
    """
    Reads all_crags.json from GCS storage, 
    turns it into a Pandas Dataframe

    Args: 
    raw_data (.json): Raw data file (Thanks Ricardo!)
    
    Returns: 
    pd.DataFrame: Extracted data 
    
    """
    json_gs_uri = gcs_url(json_blob_name)
    out_gs_uri = gcs_url("processed/crag/extracted_df.parquet")
    # Opening file via context manager using fsspec
    try:
        with fsspec.open(json_gs_uri, 'r') as f:
            all_crags = json.load(f)
            print(f"file successfully opened {json_gs_uri}")
    except Exception as E:
        print(f"Something went wrong. Error: {E}")
        return None
    
    try:
        # Normalizing the JSON data into a DataFrame
        extracted_df = pd.json_normalize(all_crags, record_path=['crags'])
        # Writing to GCS blog storage as a parquet file
        write_parquet(extracted_df, out_gs_uri)
        print(f"Wrote extracted_df → {out_gs_uri} (rows={len(extracted_df)})")
        return extracted_df
    except Exception as E:
        print(f"file was not successfully turned into dataframe. Error: {E}")
        return None