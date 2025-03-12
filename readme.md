# AWS ETL Pipeline

This repository contains an ETL pipeline using AWS services such as S3, Glue, Athena, and Airflow. The pipeline automates the process of data ingestion, transformation, and querying using Amazon Athena.

## Project Overview

### Workflow
1. **Generate Fake Data**: Using Faker to generate employee data and save it as a CSV file.
2. **Upload Data to S3**: Uploads raw data and ETL script to an S3 bucket.
3. **Delete Existing Transformed Data**: Ensures only one Parquet file exists for Athena.
4. **Trigger Glue Crawler**: Updates Glue catalog for the new dataset.
5. **Run Glue ETL Job**: Converts CSV data to Parquet format.
6. **Create Athena Table**: Queries the transformed data for analysis.

## AWS Services Used
- **Amazon S3**: Stores raw and transformed data.
- **AWS Glue**: Performs data transformation and cataloging.
- **Amazon Athena**: Queries the transformed data.
- **Apache Airflow**: Manages and schedules the ETL process.
