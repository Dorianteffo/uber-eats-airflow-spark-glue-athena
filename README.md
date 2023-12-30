# Overview
![Architecture](images/Architecture.PNG)

## Tech Stack 
* AWS Glue Data Catalog
* AWS Glue Crawler
* AWS EMR
* AWS EC2
* Apache Spark
* Airflow
* Amazon S3
* Amazon Athena
* SQL
* Python

## Project Overview 
In this project, we created an entire workflow orchestrated with Airflow. The workflow involves uploading CSV files and executing a Spark job in Python on an S3 bucket. It creates an EMR Cluster to run the Spark job, which cleans the data and loads it into another S3 bucket in Avro format with the appropriate data model. Additionally, it creates a Glue Crawler and a Data Catalog to facilitate querying the resulting data with AWS Athena. 

## Data Model 
![Data Model](model/Data_model.PNG)

## Airflow DAG 
![Airflow dag](images/airflow_dag.PNG)