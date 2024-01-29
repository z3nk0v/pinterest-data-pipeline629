# Databricks notebook source
# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

# COMMAND ----------

# Specify file type to be csv
file_type = "csv"
# Indicates file has first row as the header
first_row_is_header = "true"
# Indicates file has comma as the delimeter
delimiter = ","
# Read the CSV file to spark dataframe
aws_keys_df = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load("/FileStore/tables/authentication_credentials.csv")

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, FloatType, StringType
from pyspark.sql.functions import from_json, col, base64, regexp_replace, when, lit, array, concat

# Define Spark session
spark = SparkSession.builder.appName("StreamingExample").getOrCreate()

# Function to read data from Kinesis stream
def read_kinesis_stream(stream_name, region):
    return (
        spark.readStream
        .format("kinesis")
        .option("streamName", stream_name)
        .option("regionName", region)
        .option("initialPosition", "TRIM_HORIZON")
        .option("format", "json")
        .option("awsAccessKey", ACCESS_KEY)
        .option("awsSecretKey", SECRET_KEY)
        .option("inferSchema", "true")
        .load()
    )

# Function to deserialize JSON data
def deserialize_json(df, schema):
    return df.withColumn("cast_data", from_json(col("data").cast("string"), schema)).select("cast_data.*")

# Read data from Kinesis streams
kinesisStreamName_pin = "streaming-0ea7b76ff169-pin"
kinesisStreamName_geo = "streaming-0ea7b76ff169-geo"
kinesisStreamName_user = "streaming-0ea7b76ff169-user"

regionName = "us-east-1"

streaming_df_pin_kinesis = read_kinesis_stream(kinesisStreamName_pin, regionName)
streaming_df_geo_kinesis = read_kinesis_stream(kinesisStreamName_geo, regionName)
streaming_df_user_kinesis = read_kinesis_stream(kinesisStreamName_user, regionName)

# Define schemas for the tables
schema_pin = StructType([
    StructField("index", IntegerType()),
    StructField("unique_id", StringType()),
    # ... (add other fields as needed)
])

schema_geo = StructType([
    StructField("ind", IntegerType()),
    StructField("timestamp", TimestampType()),
    # ... (add other fields as needed)
])

schema_user = StructType([
    StructField("ind", IntegerType()),
    StructField("first_name", StringType()),
    # ... (add other fields as needed)
])

# Deserialize JSON data
streaming_df_pin = deserialize_json(streaming_df_pin_kinesis, schema_pin)
streaming_df_geo = deserialize_json(streaming_df_geo_kinesis, schema_geo)
streaming_df_user = deserialize_json(streaming_df_user_kinesis, schema_user)

# Transform and clean the data for streaming_df_pin
streaming_df_pin = (
    streaming_df_pin
    .withColumn("description", when((col("description").isin("Untitled", "No description available", "No description available Story format")), "None").otherwise(col("description")))
    .withColumn("tag_list", when((col("tag_list").isin("N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e", "0")), "None").otherwise(col("tag_list")))
    .withColumn("title", when((col("title") == "No Title Data Available"), "None").otherwise(col("title")))
    .withColumn("follower_count", when((col("follower_count") == "User Info Error"), "0").otherwise(col("follower_count")))
    .withColumn("image_src", when((col("image_src") == "Image src error."), "None").otherwise(col("image_src")))
    .withColumn("poster_name", when((col("poster_name") == "User Info Error"), "None").otherwise(col("poster_name")))
    .withColumn("downloaded", when((col("downloaded") == "None"), "0").otherwise(col("downloaded")))
    .withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
    .withColumn("follower_count", regexp_replace("follower_count", "M", "000000"))
    .withColumn("follower_count", col("follower_count").cast("int"))
    .withColumnRenamed("index", "ind")
    .withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))
    .select(col("ind"), col("unique_id"), col("title"), col("description"), col("follower_count"), col("poster_name"), col("tag_list"), col("is_image_or_video"), col("image_src"), col("downloaded"), col("save_location"), col("category"))
)

# Transform and clean the data for streaming_df_geo
streaming_df_geo = (
    streaming_df_geo
    .withColumn("coordinates", array("latitude", "longitude"))
    .drop("latitude", "longitude")
    .withColumn("timestamp", col("timestamp").cast("timestamp"))
    .select(col("ind"), col("country"), col("coordinates"), col("timestamp"))
)

# Transform and clean the data for streaming_df_user
streaming_df_user = (
    streaming_df_user
    .withColumn('user_name', concat(col("first_name"), lit(' '), col("last_name")))
    .drop("first_name", "last_name")
    .withColumn("date_joined", col("date_joined").cast("timestamp"))
    .select(col("ind"), col("user_name"), col("age"), col("date_joined"))
)

# Display the transformed dataframes
display(streaming_df_pin)
display(streaming_df_geo)
display(streaming_df_user)
