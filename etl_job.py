import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as F

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Glue and Spark context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 source details
S3_SOURCE_PATH = 's3://employee-imam/raw-data/employee.csv'
S3_TARGET_PATH = 's3://employee-imam/transformed-data/'
ATHENA_DATABASE = 'employee_db'
ATHENA_TABLE = 'employeeemployee_csv'

# Read data from S3 into a DynamicFrame
dyframe = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [S3_SOURCE_PATH]}
)

# Convert DynamicFrame to DataFrame for transformations
df = dyframe.toDF()

# Rename columns and clean phone number
df = df.withColumnRenamed("job", "job_title")\
       .withColumnRenamed("company", "company_name")\
       .withColumnRenamed("email", "email_address")\
       .withColumnRenamed("city", "city_name")\
       .withColumnRenamed("country", "country_name")

# Convert salary from USD to IDR
df = df.withColumn("salary_idr", (F.col("salary") * 15000).cast("BIGINT")) \
       .withColumn("salary", F.col("salary").cast("BIGINT"))

# Remove non-numeric characters from phone number
df = df.withColumn("phone", F.regexp_replace(F.col("phone"), "[^0-9]", ""))

# Convert DataFrame back to DynamicFrame
dyframe_transformed = DynamicFrame.fromDF(df, glueContext, "transformed_df")

# Write transformed data back to S3 in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=dyframe_transformed,
    connection_type="s3",
    format="parquet",
    connection_options={"path": S3_TARGET_PATH, "partitionKeys": []}
)

job.commit()