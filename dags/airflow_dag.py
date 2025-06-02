from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import pandas as pd
from functions import extract, transform, clean, fetch_weather_data, clean_weather_data, load

# Creating default arguments
default_args = {
    'owner': 'Anonymous',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='crag_etl', 
    default_args=default_args,
    description='Runs crag ETL pipeline',
    schedule='@daily',
    start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=['weather', 'DuckDB', 'ETL', 'crag', 'rockclimbing']
) as etl_dag:
    
    def run_crag_pipeline():
        """
        Amalgamates all ETL functions into one
        """
        # Extracting raw data from all_crags.json
        extracted_df = extract('all_crags.json')

        # Transforming raw data, exploding and normalising Pd.DataFrame
        transformed_df = transform(extracted_df)

        # Cleans transformed data
        crag_df = clean(transformed_df)

        # Extracts weather data via API call
        weather_df = fetch_weather_data(crag_df)

        # Cleans weather data
        cleaned_weather_df = clean_weather_data(weather_df)

        # Loading everything to DuckDB
        load(crag_df, cleaned_weather_df)

    etl_task = PythonOperator(
        task_id='run_etl_pipeline',
        python_callable=run_crag_pipeline
    )
