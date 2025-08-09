import pandas as pd

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
    
        transformed_df.to_parquet('data/processed/transformed_df.parquet', index=None)
        print(f"file successfully normalized. Dataframe has {transformed_df.shape}")
        return transformed_df
        
    except Exception as e:
        print(f"Transformation unsuccessful: {e}")
        return None