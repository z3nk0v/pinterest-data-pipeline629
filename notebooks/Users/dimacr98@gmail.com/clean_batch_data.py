# Databricks notebook source
# MAGIC %md
# MAGIC ##Clean df_pin Dataframe

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_replace

display(df_pin)

def add_nones_to_df(dataframe, column, value_to_replace)
    '''converts matched values to None'''
dataframe = dataframe.withColumn(column, when(col(column).like(value_to_replace), None).otherwise(col(column)))
    return dataframe

columns_and values for none = {
    'description' : 'No description available',
    'follower_count' : 'User Info Error',
    'image_src' : 'Image src error',
    'poster_name' : 'User Info Error',
    'tag_list' : 'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e',
    'Title' : 'No title Data Available'
}

           


for key, value in columns_and_values_for_null.items():
df_pin = add_nulls_to_dataframe_column(df_pin, key, value)

# Converts K's into 1000s and M's into 1,000,000
df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "M", "000000"))


# change the datatype of the "follower_count" column to int
df_pin = df_pin.withColumn("follower_count", col("follower_count").cast('int'))


# convert save_location column to include only the save location path
df_pin = df_pin.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))


# rename the index column to ind
df_pin = df_pin.withColumnRenamed("index", "ind")



df_pin = df_pin.withColumnRenamed ('index','ind')

new_pin_column_order = [
    "ind",
    "unique_id",
    "title",
    "description",
    "follower_count",
    "poster_name",
    "tag_list",
    "is_image_or_video",
    "image_src",
    "save_location",
    "category"
]
df_pin = df_pin.select(new_pin_column_order)

print (df_pin)





# COMMAND ----------

# MAGIC %md
# MAGIC ##Clean df_geo DataFrame 
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import array

display(df_geo)

#Creates new column coordinates using 
df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))


df_geo = df_geo.drop("latitude", "longitude")


# change the datatype of the "timestamp" column to timestamp
df_geo = df_geo.withColumn("timestamp", col("timestamp").cast("timestamp"))


df_geo = df_geo.select(col("ind"), col("country"), col("coordinates"), col("timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Clean df_user DataFrame
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import concat

display(df_user)

#Combines the first name and last name to create a username
df_user = df_user.withColumn('user_name', concat(df_user.first_name, df_user.last_name))

#drops first and last name columns
df_user = df_user.drop("first_name", "last_name")

# change the datatype of the "date_joined" column to timestamp
df_user = df_user.withColumn("date_joined", col("date_joined").cast("timestamp"))


df_user = df_user.select(col("ind"), col("user_name"), col("age"), col("date_joined"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Querying the Data
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Find the most popular category in each country
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

df_most_popular_category_by_country = (
    df_pin.join(df_geo, on='ind')
    .groupBy('country', 'category')
    .agg(F.count('*').alias('count'))
    .groupBy('country')
    .agg(
        F.struct(F.max('count').alias('category_count'), F.first('category').alias('category'))
        .alias('max_count')
    )
    .select('country', 'max_count.category', 'max_count.category_count')
    .withColumnRenamed("category_count", "count")
)

df_most_popular_category_by_country.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Find the most popular category by year

# COMMAND ----------

# Join df_pin with df_geo on the 'ind' column and filter by timestamp range
df_most_popular_category_by_year = (
    df_pin.join(df_geo, on='ind')
    .filter((F.year('timestamp') >= 2018) & (F.year('timestamp') <= 2022))
)

# Group by year and category, and count the number of occurrences
df_most_popular_category_by_year = (
    df_most_popular_category_by_year.groupBy(F.year('timestamp').alias('post_year'), 'category')
    .agg(F.count('*').alias('category_count'))
)

# Find the most popular category for each year
window_spec = Window.partitionBy('post_year').orderBy(F.desc('category_count'))

df_most_popular_category_by_year = (
    df_most_popular_category_by_year.withColumn('rank', F.rank().over(window_spec))
    .filter(F.col('rank') == 1)
    .select('post_year', 'category', 'category_count')
)

# Show the result
df_most_popular_category_by_year.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Find the user with the most followers in each country

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Find the user with the most followers in each country
window_spec = Window.partitionBy('country').orderBy(F.desc('follower_count'))

df_most_followers_by_country = (
    df_pin.join(df_geo, on='ind')
    .groupBy('country', 'poster_name')
    .agg(F.max('follower_count').alias('follower_count'))
    .withColumn('rank', F.rank().over(window_spec))
    .filter(F.col('rank') == 1)
    .select('country', 'poster_name', 'follower_count')
)

df_most_followers_by_country.show()

# Find the country with the user that has the most followers
df_most_followers_country = (
    df_most_followers_by_country.groupBy('country')
    .agg(F.max('follower_count').alias('follower_count'))
    .orderBy(F.desc('follower_count'))
    .limit(1)
    .select('country', 'follower_count')
)

df_most_followers_country.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Find the most popular category for different age groups

# COMMAND ----------

from pyspark.sql import functions as F

# Create age groups using when-otherwise
df_user_with_age_group = (
    df_pin.join(df_user, on="ind")
    .withColumn("age_group", F.when(df_user.age.between(18, 24), "18-24")
                .when(df_user.age.between(25, 35), "25-35")
                .when(df_user.age.between(36, 50), "36-50")
                .when(df_user.age > 50, "+50")
                .otherwise("Unknown"))
)

# Group by age_group and category, and count the number of occurrences
df_category_count_by_age = (
    df_user_with_age_group.groupBy("age_group", "category")
    .agg(F.count("*").alias("category_count"))
    .groupBy("age_group")
    .agg(
        F.struct(F.max("category_count").alias("category_count"), 
                 F.first("category").alias("category"))
        .alias("max_count")
    )
    .select("age_group", "max_count.category", "max_count.category_count")
)

df_category_count_by_age.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Find the median follower count for different age groups

# COMMAND ----------

from pyspark.sql import functions as F

# Calculate the median follower count by age group
df_median_follower_count_by_age = (
    df_user_with_age_group.groupBy("age_group")
    .agg(F.expr("percentile_approx(follower_count, 0.5, 1000000)").alias("median_follower_count"))
    .select("age_group", "median_follower_count")
)

df_median_follower_count_by_age.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Find how many users have joined each year

# COMMAND ----------

from pyspark.sql import functions as F

# Join df_user with df_geo on 'ind', select relevant columns, and filter by timestamp range
df_user_geo_joined = (
    df_user.join(df_geo, on='ind')
    .select(F.year('timestamp').alias('post_year'), 'date_joined')
    .where((F.year('timestamp') >= 2015) & (F.year('timestamp') <= 2020))
)

# Group by post_year and count the number of users joined
df_users_joined_by_year = (
    df_user_geo_joined.groupBy("post_year")
    .agg(F.count("*").alias("number_users_joined"))
)

df_users_joined_by_year.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 7. Find the median follower count of users based on their joining year

# COMMAND ----------

from pyspark.sql import functions as F

# Filter users who joined between 2015 and 2020
df_user_joined = df_user.filter((df_user.date_joined >= '2015-01-01') & (df_user.date_joined <= '2020-12-31'))

# Extract the year from the timestamp column in the df_geo table
df_geo_year = df_geo.withColumn("post_year", F.year("timestamp"))

# Join df_user_joined with df_geo_year and df_pin to get follower count for each user
df_join_pin = df_user_joined.join(df_geo_year, on="ind").join(df_pin, on="ind")

# Calculate the median follower count for each year
df_median_followers_by_post_year = (
    df_join_pin.groupBy("post_year")
    .agg(F.expr("percentile_approx(follower_count, 0.5, 1000000)").alias("median_follower_count"))
)

# Display the results
df_median_followers_by_post_year.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 8. Find the median follower count of users based on their joining year and age

# COMMAND ----------

from pyspark.sql import functions as F

# Filter users joined between 2015 and 2020
df_user_filtered = df_user.filter((df_user.date_joined >= "2015-01-01") & (df_user.date_joined < "2021-01-01"))

# Join the required dataframes
df_join2 = (
    df_pin.join(df_user_filtered, on="ind")
    .join(df_geo.select("ind", F.year("timestamp").alias("post_year")), on="ind")
    .join(df_user_with_age_group.select("age_group", "ind"), on="ind")
)

# Calculate the median follower count by age group and post year
df_median_follower_by_year_joined = (
    df_join2.groupBy("age_group", "post_year")
    .agg(F.expr("percentile_approx(follower_count, 0.5, 1000000)").alias("median_follower_count"))
)

# Print the results
df_median_follower_by_year_joined.show()