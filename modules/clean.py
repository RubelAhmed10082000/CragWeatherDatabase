import numpy as np
import pandas as pd
from modules.gcs_io import gcs_url, read_parquet, write_parquet


def clean(src_blob = "processed/crag/transformed_df.parquet",
          dst_blob = "cleaned/crag/crag_df.parquet"):
    
    """
    
    Cleans transformed dataframe. Produces new columns, applies appropriate data types, drops unneeded columns and applies np.nan
    Exports to GCS blob storage as .parquet file

    Args:
    
    transformed_data (pd.DataFrame): The transformed data. Result of transform() function
    
    Returns: 
    
    cleaned_data (pd.DataFrame): Dataframe that has been cleaned
    
    """

    src, dst = gcs_url(src_blob), gcs_url(dst_blob)

    try:
        crag_df = read_parquet(src)
    except FileNotFoundError:
        print(f"Source not found {src}")
        return None

    if crag_df is None:
        print("No data to clean")
        return None
    
    try:
        crag_df = crag_df.replace("",np.nan)
        # Dropping unnecessary columns
        drop_cols = ['direction', 'is_hill', 'slug', 'difficulty', 'stars']

        crag_df = crag_df.drop(columns=[c for c in drop_cols if c in crag_df.columns], errors="ignore")     
        
        # Removing any rows where the longitude and latitude are 0
        for c in ("longitude","latitude"):
            if c in crag_df.columns:
                crag_df[c] = pd.to_numeric(crag_df[c], errors="coerce")   
        crag_df = crag_df.dropna(subset=[c for c in ("longitude","latitude") if c in crag_df.columns])   
        if "longitude" in crag_df.columns and "latitude" in crag_df.columns:
            crag_df = crag_df.loc[~((crag_df["longitude"] == 0) | (crag_df["latitude"] == 0))]
        
        
        # Replacing 'Summit' or 'summit' with NaN, 
        # Replacing blank sector names with 'Main Area'
        if "sector_name" in crag_df.columns:
            crag_df["sector_name"] = crag_df["sector_name"].replace(['Summit', 'summit'], np.nan)
            crag_df["sector_name"] = crag_df["sector_name"].fillna("Main Area")
       
        # Creating a list of all UK safety grades
        if "grade" in crag_df.columns:

            uk_safety_grade = ['M', 'D', 'HD', 'VD', 'HVD', 'MS',
                    'S', 'HS', 'MVS', 'VS', 'HVS'] + [f'E{i}' for i in range(1, 12)]
        
        
        # Now split cleaned grade into safety and difficulty
        def extract_safety_and_difficulty(grade):
            """
            Creates two columns. Safety grade and difficulty grade. Does this by splitting the grade column.

            Args: grade column

            Result:

            Safety
            Difficulty
            
            """

            # Returns none if column is not string
            if not isinstance(grade, str):
                return (np.nan, np.nan)
            # Splits grade column into two different parts
            parts = grade.split(' ', 1)
            # If part[0] is in the uk_safety_grade list then it is added to the safety column 
            if parts[0].upper() in uk_safety_grade:
                safety = parts[0].upper()
                # The part[1] is added into the difficulty column.
                difficulty = parts[1] if len(parts) > 1 else np.nan
            else:
                # If part[0] is not in the safety_grade_list it is np.nan
                safety = np.nan
                difficulty = grade
            return safety, difficulty
        
        # Applying function to the grade column, creating two new columns
        crag_df[['safety_grade', 'difficulty_grade']] = crag_df['grade'].apply(lambda x: pd.Series(extract_safety_and_difficulty(x)))
        
        # 'MOD' is turned to 'M' to fit the safety_grading standard
        crag_df['safety_grade'] = crag_df['safety_grade'].replace('MOD','M')

        # Removing any instance of 'none' in difficulty_grade column
        crag_df['difficulty_grade'] = crag_df['difficulty_grade'].str.replace(r'\bnone\b', '', case=False, regex=True)

        # Removing any instance of 'project' in difficulty_grade column
        crag_df['difficulty_grade'] = crag_df['difficulty_grade'].str.replace(r'\bproject\b', '', case=False, regex=True)

        # Removing any instance of '?' in difficulty_grade column
        crag_df.loc[crag_df['difficulty_grade'].str.contains(r'\?', na=False), 'difficulty_grade'] = np.nan
        
        # Dropping grade column
        crag_df = crag_df.drop(columns=['grade'])
        
        # Setting ID as index
        crag_df = crag_df.set_index('crag_id')

        # Changing columns to relevant data types
        astype_crag = {'crag_name': 'string', 'county': 'string', 'country': 'string', 'rocktype': 'category', 'sector_name': 'string',
                        'type': 'category', 'longitude': 'float64', 'latitude': 'float64', 'route_name': 'string', 'difficulty_grade':'string','safety_grade':'category'}
        
        crag_df = crag_df.astype(astype_crag)
        
        # Exporting function result to GCS blob storage as a parquet file
        write_parquet(crag_df, dst)
        print(f"file successfully cleaned. Dataframe has {crag_df.shape}")
        return crag_df
    except Exception as e:
        print(f"Cleaning unsuccessful: {e}")
        return None

