**Crag Weather Database**

I created a data pipeline that ingests both rock climbing location data and 7 day hourly weather forecasts that is then matched in a database.
The goal is to allow climbers to plan their outdoor climbing trips by filtering which locations have the best weather for climbing

**How It's Made:**

Tech used: 

Python, SQL, DUCKDB, Pandas, Openmeteo-Requests, Docker, Airflow, Great-Expectations, Numpy, Dateutil

Data flow:

The project started of with Ricardo Nunes, who I met on Reddit and who kindly volunteered to webscrape England's climbing location data from the popular climbing website UKC.com. He scraped data using requests and the output was a nested .json dictionary

After receiving the .json file I created an extract() function which was used to turn convert the .json file into a Pandas Dataframe. A seperate transform() file was used to normalize and explode columns so that Pandas Datframe had the appropriate columnar shape

The clean() function was then used to fill in any Nulls, seperate columns and create new columns where appropriate

This created a Dataframe consisting of all known climbing locations, with their respective routes, sectors, grades, safety grades, rocktype, climbing type etc. This information can be used by climbers to filter locations based on their climbing preference / ability in addition to the weather. This Dataframe consists of over 127,000 rows of data

Next, my fetch_weather_data() function made an API call using the OpenMeteo weather API. I passed both Longitude and Latitude columns from my climbing location Dataframe as an argument to the API call which allowed me to receive weather data for every climbing location in England. The call returns a Dataframe with over 750,000 rows of data
 
clean_weather_data() does simple cleaning on the weather Dataframe, renaming some columns for better readabilty

Both climbing and weather dataframes were used ingested into a DUCKDB in-memory database, I adopted a simple database schema. Credit to reddit user No-Adhesiveness-6921 for helping me develop this schema

![ktflvqnw1r0f1](https://github.com/user-attachments/assets/57a316f2-b8d6-46d4-a7e7-e4190578f390)

I used created a simple Airflow DAG to run the entire pipeline automatically everyday at 1am. Docker was used to create a virtual enviroment for my which both my DAG and my pipeline will run

Great-Expectations is currently being worked on, this will validation both the schema as well as column datatypes for both the climbing and weather database before being ingested into DUCKDB. The validation will then be intergrated into the Airflow DAG

**Lessons Learned / Challenges:**

Webscraping - I tried to webscrape the data myself but ultimately failed. I believe it is a skill that I need to work on because it is an aspect with a lot of depth. I realised that many websites have protections against webscraping

Docker - Setting up a working Docker enviroment was also challenging due to its depth, complexity and the need to understand file structures. It took me a lot of trial and error to get the Docker container, and by extension an Airflow DAG, working

**To Do**:

Great-Expectations - Need to create Great-Expectation validation suite and intergrate validation checkpoints into my Airflow DAG

**Next Steps**:

FastAPI - I plan on learning FastAPI and Pydantic to be able create a frontend to display the locations and weather data to an end-user

Scaling Up - I want to scale up my pipeline by adding climbing locations from other countries. However, this may require SPARK as well as a cloud based data warehouse




