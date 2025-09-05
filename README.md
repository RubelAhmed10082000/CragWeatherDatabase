![image](https://github.com/RubelAhmed10082000/CragWeatherDatabase/blob/f435616046ca3d66af7f4d56b1e372e2bb489b87/Front%20Page.PNG)

**CragCast**

Cloud‑native platform mapping every UK rock‑climbing crag to 7‑day hourly forecasts for fast, accurate trip planning.

**Status (05 Sep 2025)**: 

• ~4,000 UK crags 
• FastAPI read‑only API in progress 
• frontend next

***TLDR**

Shipped a cloud‑native data platform mapping 4,000 UK crags to 7‑day hourly forecasts (672k active rows)

Designed 3NF de-normalized snowflake schema. Implemented primary keys, unique keys for data intergrity and indexes for low‑latency reads. Includes idempotent upserts, hour‑floored timestamps and logs for monitoring batches

Deployed hourly and daily jobs on GCP Cloud Run with Secret Manager + TLS verify‑full; API serves DB‑only reads

![image](https://github.com/RubelAhmed10082000/CragWeatherDatabase/blob/f435616046ca3d66af7f4d56b1e372e2bb489b87/Crag%20Details.PNG)

**How it was made**

Scraped UKC.com for details on all outdoor climbing location and routes

ETL pipeline transformed data and matched outdoor rockclimbing locations with weather data

Built and hosted a 3NF Snowflake schema using PostgreSQL

Shipped Cloud Run Jobs (hourly upsert, daily backfill) via Cloud Scheduler with OAuth triggers.

Implemented a DB‑only FastAPI layer (no third‑party calls on the read path)

Solved real infra issues (TLS trust in containers, Scheduler→Run 404/OAuth wiring)


**Skills Used**

Data Modelling: schema design, indexing, normalization and keys

Cloud pragmatism: deployed, monitored, debugged GCP infra such as GCP Blob Storage, Google Cloud Run and Google Scheduler

Data engineering: idempotent merges, lineage fields, stable windows, ETL pipeline, data cleaning etc

Software Engineering: seperation of concerns, unit and intergrations testing, perfomance optmiziation etc.

Product sense: built for end‑user speed and clarity, not a demo.


**Next Steps**

Frontend - I plan on learning FastAPI to be able create a frontend to display the locations and weather data to an end-user

Scaling Up - I want to scale up my pipeline by adding climbing locations from other countries. However, this may require SPARK as well as a cloud based data warehouse

Multiple DAGs - As you can see in the picture, I have only one DAG instance of which includes my entire ETL pipeline. I may want to make each stage of the pipeline its own instance in order to enhance modularity and be able to monitor the DAG.

***Tech Stack**
• Python 
• SQL 
• PostgreSQL
• FastAPI 
• Flask
• Docker 
• Cloud Run Jobs 
• Cloud Scheduler 
• Secret Manager 
• Open‑Meteo
• Great Expectations

Deep dive for engineers: see the technical README (TECHNICAL.md) with schema, SQL, and deployment details
