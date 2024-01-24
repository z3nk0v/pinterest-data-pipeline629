# Databricks notebook source
# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

#Specify file type, is the first row is a header and the delimiter
file_type = 'csv'
first_row_is_header = "true"
delimiter = ","


# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load("/FileStore/tables/authentication_credentials.csv")

# # Define the path to the Delta table
# delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# AWS S3 bucket name
AWS_S3_BUCKET = "user-0ea7b76ff169-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/mount_to_s3"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

display(dbutils.fs.ls("/mnt/mount_s3_to_db/../.."))


# Disable format checks during the reading of Delta tables
SET spark.databricks.delta.formatCheck.enabled=false

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/mount_s3_to_db/topics/0ea7b76ff169.pin/partition=0/*.json"/" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_pin = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_pin)

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/mount_s3_to_db/topics/0ea7b76ff169.geo/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_geo = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_geo)


# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/mount_s3_to_db/topics/0ea7b76ff169.user/partition=0/*.json"
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_user = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_user)