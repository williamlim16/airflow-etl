# Purpose
This project is a simple project demonstrating the ETL process using apache [airflow](https://airflow.apache.org/).  

# Steps
There are a few main steps which are
- Extract: extracting the data from the data source
- Transform: transforming the data for our use case
- Load: loading our data to our data warehouse solution

# Context

## Data warehouse solution
There are many other options which has its own advantages and disadvantages. In this sample project, I used PostgreSQL as my data warehouse solution due to its flexibility.

## Dataset  
The dataset that I am using is from kaggle which can be found through this [link](https://www.kaggle.com/datasets/karkavelrajaj/amazon-sales-dataset?resource=download).

## Docker
I used docker so that this demo can be easily reproduced regardless of the computer that you are using. You can simply run "docker compose up -d" command to get the exact environment.

