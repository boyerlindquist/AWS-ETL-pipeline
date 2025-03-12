from airflow import DAG
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from generate_data import generate_fake_data


# Define variables
S3_BUCKET = "employee-imam"
S3_PREFIX = "raw-data/"
LOCAL_CSV_FILE = "/home/imam/data_engineer/cloud_pipeline/employee.csv"
ETL_JOB_SCRIPT = "/home/imam/data_engineer/cloud_pipeline/etl_job.py"
GLUE_CRAWLER_NAME = "employee-crawler-imam"
GLUE_JOB_NAME = "ETL_job"
ATHENA_DB = "employee_db"
ATHENA_TABLE = "employee_table"
ATHENA_OUTPUT = f"s3://{S3_BUCKET}/athena-results/"

default_args = {
    "owner": "Imam AWS",
    "start_date": days_ago(0),
    "retries": 1
}

with DAG("AWS_pipeline",
         default_args=default_args,
         schedule_interval="@daily",
         catchup=False) as dag:

    # 1. Generate fake data
    generate_data = PythonOperator(
        task_id="generate_fake_data",
        python_callable=generate_fake_data
    )

    # 2. Upload CSV to S3
    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename=LOCAL_CSV_FILE,
        dest_key=f"{S3_PREFIX}employee.csv",
        dest_bucket=S3_BUCKET,
        replace=True
    )

    # 3. Upload ETL script to S3
    upload_etl_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_etl_to_s3",
        filename=ETL_JOB_SCRIPT,
        dest_key=f"{S3_PREFIX}etl_job.py",
        dest_bucket=S3_BUCKET,
        replace=True
    )

    # 4. Delete existing parquet file in transformed-file ensuring only 1 parquet exist to load to athena
    delete_existing_transformed = BashOperator(
        task_id = "delete_existing_transfromed",
        bash_command="aws s3 rm s3://employee-imam/transformed-data/ --recursive"
    )

    # 5. Wait for new data in S3
    check_s3_data = S3KeySensor(
        task_id="check_s3_data",
        bucket_name=S3_BUCKET,
        bucket_key=f"{S3_PREFIX}employee.csv",
        timeout=600,
        poke_interval=60,
        mode="poke"
    )

    # 6. Run AWS Glue Crawler
    run_glue_crawler = GlueCrawlerOperator(
        task_id="run_glue_crawler",
        config={"Name": GLUE_CRAWLER_NAME},
        wait_for_completion=True
    )

    # 7. Run AWS Glue ETL Job
    run_glue_job = GlueJobOperator(
        task_id="run_glue_job",
        job_name=GLUE_JOB_NAME,
        wait_for_completion=True
    )

    # 8. Create Athena Table
    create_athena_table = AthenaOperator(
        task_id="create_athena_table",
        query=f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DB}.{ATHENA_TABLE} (
            first_name STRING,
            last_name STRING,
            job_title STRING,
            company_name STRING,
            salary BIGINT,
            email_address STRING,
            phone STRING,
            city_name STRING,
            country_name STRING,
            salary_idr BIGINT
        )
        STORED AS PARQUET
        LOCATION 's3://{S3_BUCKET}/transformed-data/';
        """,
        database=ATHENA_DB,
        output_location=ATHENA_OUTPUT
    )

    # Define task dependencies
    generate_data >> [upload_to_s3, upload_etl_to_s3 ] >> delete_existing_transformed >> check_s3_data >> run_glue_crawler >> run_glue_job >> create_athena_table
