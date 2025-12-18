# Data Engineer Code Exercise
Below is a simple coding exercise designed to check basic Python and SQL knowledge

# Scenario
Bloomeroo, a Melbourne-based flower business, is looking to optimise its flower growth yields. They want to use weather data to find the best spots in the city for growing.
The datasource to be used is publicly available from this microclimate sensor(https://data.melbourne.vic.gov.au/explore/dataset/microclimate-sensors-data/api/) data API.
As their dataengineer, your task is to write a command-line Python program that reads data from the weather API and loads it into a Postgres database for analysis. The analyst team needs at least 1000 records to evaluate temperature and humidity patterns by location and time.


# Current Architecture
![ArchDiagram](/OverallArch.svg)

# Prerequisites

Python: 3.10+

PostgreSQL: Cloud SQL from GCP

Tools: git, pip, psql, Visual studio Code

Network: Access to the microclimate API (public) [URL](https://data.melbourne.vic.gov.au/explore/dataset/microclimate-sensors-data/api/)

# Steps
1) Create repository in github and clone to local visual studio code.
2) Run `python scripts/fetch_microclimate.py` and load the data to (microclimate_20.csv) file
3) Prepare the database. Since I have used Cloud SQL (Postgresql) in Google cloud. Go to console.google.com login and create instance of Cloud SQL for Postgresql and in connections add IP address of local machine and get the connectivity.
4) Create user : "weatherapi_login" in Cloud SQL instance.
5) Add connection details in the .py files which connectes to Cloud Postgresql.
6) Prepare databse : 
          Execute in local terminal `python scripts/prepare_db.py <Password>`
7) Load .csv file to Postgresql table :
          Execute `python scripts/postgresql_loader.py <Password>`
8) For data quality did cleanising of data to remove NULL and duplicate :         Execute `python scripts/cleanse_proc.py <Password>`

Data Analyst Team can use weather_data table and use query [scripts/sql_query_analyst.sql](scripts/sql_query_analyst.sql)

# Future Enhancement

When this tranforms into a project , we can have a below components enahcning this into a robust Weather data extraction framework with :
 - Google Kubernetes engine running the python code or a Google Container Engine.
 - Pushing csv files to a Cloud Storage Landing Zone
 - Creating Staging , Fact/Dimension tables and Target in Google Big Query 
 - Using Looker Studio on Target Tables for enhanced Analytics

![ArchDiagram](/ArchDiagram.svg)
