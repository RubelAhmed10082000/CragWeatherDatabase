![image](https://github.com/user-attachments/assets/077c0bd1-01e6-43c9-8aa0-a2cc874e2297)

**Crag Weather Database**

I created a data pipeline that ingests both rock climbing location data and 7 day hourly weather forecasts that is then matched in a database.
The goal is to allow climbers to plan their outdoor climbing trips by filtering which locations have the best weather for climbing

**How It's Made:**

Tech used: 

Python, SQL, Pandas, JSON, DUCKDB, Pandas, Openmeteo-Requests, Docker, Airflow, Great-Expectations, Numpy, Dateutil, Pytest

Data flow:

The project started of with Ricardo Nunes, who I met on Reddit and who kindly volunteered to webscrape England's climbing location data from the popular climbing website UKC.com. He scraped data using requests and the output was a nested .json dictionary

After receiving the .json file I created an extract() function which was used to turn convert the .json file into a Pandas Dataframe. A seperate transform() file was used to normalize and explode columns so that Pandas Datframe had the appropriate columnar shape

The clean() function was then used to fill in any Nulls, seperate columns and create new columns where appropriate

This created a Dataframe consisting of all known climbing locations, with their respective routes, sectors, grades, safety grades, rocktype, climbing type etc. This information can be used by climbers to filter locations based on their climbing preference / ability in addition to the weather. This Dataframe consists of over 127,000 rows of data

Next, my fetch_weather_data() function made an API call using the OpenMeteo weather API. I passed both Longitude and Latitude columns from my climbing location Dataframe as an argument to the API call which allowed me to receive weather data for every climbing location in England.

Currently I have set this code to only retrieve the weather data for the first 50 crags. This makes testing and debugging easier. The Dataframe which returns the the weather for all crags has 710,000 rows, however, the call is much longer. When the endpoint is developed, we can use pagination to only call the weather data that is needed as well as incorporate threading. This will allow for fast access to weather data

![image](https://github.com/user-attachments/assets/f8bf3835-9ed8-4854-8b47-33576170bbd4)

clean_weather_data() does simple cleaning on the weather Dataframe, renaming some columns for better readabilty

Both climbing and weather dataframes ingested into a DUCKDB in-memory database, I adopted a simple database schema. Credit to reddit user No-Adhesiveness-6921 for helping me develop this schema

![ktflvqnw1r0f1](https://github.com/user-attachments/assets/57a316f2-b8d6-46d4-a7e7-e4190578f390)

Great-Expectations validations was used to check data intergrity on both schema, row and column level. The expectations were intergrating directly into the pipeline, in the form of the run_expectations() function, instead of being an Airflow Operator

I created a simple Airflow DAG to run the entire pipeline automatically everyday at 1am. Docker was used to create a virtual enviroment from my which my DAG will run

![image](https://github.com/user-attachments/assets/e03a0fd9-23ea-4de1-8469-6036c0e8b323)

Pytest was used to test the code and create edge cases. This was done on both a unit and feature level

**Lessons Learned / Challenges:**

Webscraping - I tried to webscrape the data myself but ultimately failed. I believe it is a skill that I need to work on because it is an aspect with a lot of depth. I realised that many websites have protections against webscraping

Docker - Setting up a working Docker enviroment was also challenging due to its depth, complexity and the need to understand file structures. It took me a lot of trial and error to get the Docker container, and by extension an Airflow DAG, working

Great Expectations - Great Expectations is a package that allows for data validation. However, I have found this package cumbersome. For example, setting up validation takes multiple steps and it is required to add validations to a suite one at a time. I believe this pacakge will be a key bottleneck when expanding.


**Next Steps**:

Frontend - I plan on learning FastAPI to be able create a frontend to display the locations and weather data to an end-user

Scaling Up - I want to scale up my pipeline by adding climbing locations from other countries. However, this may require SPARK as well as a cloud based data warehouse

Multiple DAGs - As you can see in the picture, I have only one DAG instance of which includes my entire ETL pipeline. I may want to make each stage of the pipeline its own instance in order to enhance modularity and be able to monitor the DAG.

**How To Get It Working**:

- Please install the needed packages via requirements.txt
  
- The extract() functions takes in 'all_crags.json' as an argument

- Execution code would look something like this:

 extracted_df = extract('dags/Files/all_crags.json')
 
 transformed_df = transform(extracted_df)
 
 crag_df = clean(transformed_df)
 
 weather_df = fetch_weather_data()
 
 cleaned_weather_df = clean_weather_data(weather_df)
 
 run_expectations()
 
 load_result = load(crag_df, cleaned_weather_df)

In order to get the Airflow DAG working, please to refer to this documenation: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

In order to get the DAG to work your PC must allow virtualisation and you must have the Docker application installed and open on your PC.




