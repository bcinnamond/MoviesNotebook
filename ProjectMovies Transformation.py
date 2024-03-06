# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

# DBTITLE 1,Mounting the file storage
dbutils.fs.mount(
    source='wasbs://project-movies-data@rcmoviesdatasa.blob.core.windows.net',
    mount_point='/mnt/project-movies-data',
    extra_configs = {'fs.azure.account.key.rcmoviesdatasa.blob.core.windows.net': dbutils.secrets.get('projectmoviesscope', 'storageAccountKey')}
)


# COMMAND ----------

# DBTITLE 1,List the content of the container
# MAGIC %fs
# MAGIC ls "/mnt/project-movies-data"
# MAGIC

# COMMAND ----------

# DBTITLE 1,read in the action csv file
action = spark.read.format("csv").load("/mnt/project-movies-data/raw-data/action")

# COMMAND ----------

# DBTITLE 1,display the action csv file
action.show()

# COMMAND ----------

# DBTITLE 1,Load in the action.csv file and use first row as header
action = spark.read.format("csv").option("header", "true").load("/mnt/project-movies-data/raw-data/action")

# COMMAND ----------

# DBTITLE 1,display the contents of the action.csv file
action.show()

# COMMAND ----------

# DBTITLE 1,load in the adventure dataset
action = spark.read.format("csv").option("header", "true").load("/mnt/project-movies-data/raw-data/adventure.csv")

# COMMAND ----------

# DBTITLE 1,load in the horror dataset
action = spark.read.format("csv").option("header", "true").load("/mnt/project-movies-data/raw-data/horror.csv")

# COMMAND ----------

# DBTITLE 1,load in the scfi dataset
action = spark.read.format("csv").option("header", "true").load("/mnt/project-movies-data/raw-data/scifi.csv")

# COMMAND ----------

# DBTITLE 1,load in the thriller dataset
action = spark.read.format("csv").option("header", "true").load("/mnt/project-movies-data/raw-data/thriller.csv")

# COMMAND ----------

# DBTITLE 1,display the schema
action.printSchema()

# COMMAND ----------

# DBTITLE 1,Change the data type of the rating column
action = action.withColumn("rating", col("rating").cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,View action movies by highest rating
all_movies_rated_highest = action.orderBy("rating", ascending=False).limit(20).show()

# COMMAND ----------

# DBTITLE 1,Set a smaller record limit
all_movies_rated_highest = action.orderBy("rating", ascending=False).select("movie_name", "genre", "rating").limit(15).show()

# COMMAND ----------

# DBTITLE 1,View action movies which also have the genre comedy
comedy_movies = action.filter(col("genre").contains("Comedy")).limit(15).show()

# COMMAND ----------

# DBTITLE 1,Write transformed data to a container
action.write.option("header",'true').csv("/mnt/project-movies-data/transformed-data/action")

# COMMAND ----------

# DBTITLE 1,overwrite file in a container
action.write.mode("overwrite").option("header",'true').csv("/mnt/project-movies-data/transformed-data/action")

# COMMAND ----------

# DBTITLE 1,Change year column to DateType
action = action.withColumn("year", col("year").cast(DateType()))

# COMMAND ----------

# DBTITLE 1,Remove the day and month from the datetype field
from pyspark.sql.functions import col, date_format

action = action.withColumn("year", date_format(col("year"), "yyyy"))

# COMMAND ----------

all_movies_rated_highest = action.orderBy("rating", ascending=False).select("movie_name", "genre", "rating").limit(15).show()

# COMMAND ----------

# DBTITLE 1,Read the CSV file as a Spark DataFrame
action = spark.read.format("csv") \
                  .option("header", "true") \
                  .option("inferSchema", "true") \
                  .load("/mnt/project-movies-data/raw-data/action") \
                  .createOrReplaceTempView("temp_table")

# COMMAND ----------

# DBTITLE 1,Create a Spark table from the temporary view - temp_table
spark.sql("CREATE TABLE IF NOT EXISTS actiontb USING parquet AS SELECT * FROM temp_table")

# COMMAND ----------

# DBTITLE 1,Query for movies with rating = 8
query_result = spark.sql("SELECT year, movie_name, rating FROM actiontb WHERE rating = 8")

# COMMAND ----------

# DBTITLE 1,Import Plotly Express for visualisation
import plotly.express as px

# COMMAND ----------

# DBTITLE 1,Create a Pandas DataFrame for plotting
pandas_df = query_result.toPandas()

# COMMAND ----------

# DBTITLE 1,Group by year, count movies, and create a DataFrame with "year" and "count" columns
grouped_df = pandas_df.groupby("year").size().to_frame(name="count").reset_index()

# COMMAND ----------

# DBTITLE 1,Create the bar chart using Plotly
fig = px.bar(grouped_df, x="year", y="count")
fig.update_layout(width=900, height=600)  # Set plot size
fig.show()  # Display the plot

# COMMAND ----------

# DBTITLE 1,Choosing a colour scale
fig = px.bar(grouped_df, x="year", y="count", color="count", color_continuous_scale="Viridis")

# COMMAND ----------

# DBTITLE 1,Set plot size
fig.update_layout(width=900, height=600)

# COMMAND ----------

# DBTITLE 1,Display the plot
fig.show()
