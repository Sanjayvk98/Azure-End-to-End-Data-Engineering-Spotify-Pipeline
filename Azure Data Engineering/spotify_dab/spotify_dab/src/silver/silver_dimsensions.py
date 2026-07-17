# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
import sys

project_path = os.path.join(os.getcwd(),'..','..')
sys.path.append(project_path)

print(project_path)


# COMMAND ----------

from utils.transformations import reusable  # Verify if 'reusable' exists in transformations.py or import the correct name

# COMMAND ----------

# MAGIC %md
# MAGIC  **Dim Users**

# COMMAND ----------

df = spark.read.format("parquet").load("abfss://bronze@azuredataengst.dfs.core.windows.net/DimUser")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Autoloader**

# COMMAND ----------

df_user = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","abfss://silver@azuredataengst.dfs.core.windows.net/DimUser/checkpoint")\
    .option("schemaEvolutionMode","addNewColumns")\
    .load("abfss://bronze@azuredataengst.dfs.core.windows.net/DimUser")

# COMMAND ----------

# Pass the correct camelCase parameter 'checkpointLocation' with a valid directory string
#display(df_user, checkpointLocation = "abfss://silver@azuredataengst.dfs.core.windows.net/DimUser/_checkpoint_display")

# COMMAND ----------

df_user_str = df_user.withColumn("user_name", upper(col("user_name")))

# Let display() handle the mode automatically, or explicitly keep append without conflicting configurations
#display(df_user_str, checkpointLocation = "abfss://silver@azuredataengst.dfs.core.windows.net/DimUser/_checkpoint_display_v2")

# COMMAND ----------

df_user_obj = reusable()
df_user = df_user_obj.dropColumns(df_user, ['_rescued_data'])

# FIX: Remove the first df_user argument here
df_user = df_user.dropDuplicates(['user_id']) 

# Note: Since df_user is a streaming DataFrame, you still need your checkpointLocation to display it
#display(df_user, checkpointLocation="abfss://silver@azuredataengst.dfs.core.windows.net/DimUser/_checkpoint_display_v3")

# COMMAND ----------

# Read a static snapshot of the bronze data directly to view it as a normal table
df_static_preview = spark.read.format("parquet").load("abfss://bronze@azuredataengst.dfs.core.windows.net/DimUser")

# Apply your transformations statically just to see how they look
df_static_preview = df_static_preview.drop("_rescued_data").dropDuplicates(["user_id"]).limit(100)

display(df_static_preview)

# COMMAND ----------

df_static_preview = df_static_preview.withColumn("user_name", upper(col("user_name")))
display(df_static_preview)

# COMMAND ----------

df_user.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "abfss://silver@azuredataengst.dfs.core.windows.net/DimUser/_checkpoint_write") \
    .trigger(once=True) \
    .option("path", "abfss://silver@azuredataengst.dfs.core.windows.net/DimUser/data") \
    .toTable("spotify_data.silver.DimUser")


# COMMAND ----------

# Run this in a new cell to check your saved Silver data as a clean table
df_silver_check = spark.read.format("delta").load("abfss://silver@azuredataengst.dfs.core.windows.net/DimUser/data")
display(df_silver_check)

# COMMAND ----------

# MAGIC %md
# MAGIC **Dim Artist**

# COMMAND ----------

df_artist = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","abfss://silver@azuredataengst.dfs.core.windows.net/DimArtist/checkpoint")\
    .option("schemaEvolutionMode","addNewColumns")\
    .load("abfss://bronze@azuredataengst.dfs.core.windows.net/DimArtist")

# COMMAND ----------

#display(df_artist, checkpointLocation = "abfss://silver@azuredataengst.dfs.core.windows.net/DimArtist/_checkpoint_display")

# COMMAND ----------

df_static_prev_art = spark.read.format("parquet").load("abfss://bronze@azuredataengst.dfs.core.windows.net/DimArtist")

# COMMAND ----------

display(df_static_prev_art)

# COMMAND ----------

print(df_artist.columns)

# COMMAND ----------

df_artist.printSchema()

# COMMAND ----------

df_art_obj = reusable()

df_artist = df_art_obj.dropColumns(df_artist,['_rescued_data'])
df_artist = df_artist.dropDuplicates(['artist_id'])
df_static_prev_art = df_static_prev_art.dropDuplicates(["artist_id"])


# COMMAND ----------

display(df_artist.columns)

# COMMAND ----------

display(df_static_prev_art)

# COMMAND ----------

df_artist.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "abfss://silver@azuredataengst.dfs.core.windows.net/DimArtist/_checkpoint_write") \
    .trigger(once=True) \
    .option("path", "abfss://silver@azuredataengst.dfs.core.windows.net/DimArtist/data") \
    .toTable("spotify_data.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC To create table in Catalog
# MAGIC     """df_artist.writeStream.format("delta")\
# MAGIC     .outputMode("append")\
# MAGIC     .option("checkpointLocation","abfss://silver@azuredataengst.dfs.core.windows.net/DimArtist/checkpoint")\
# MAGIC     .trigger(once=True)\
# MAGIC     .option(path,"abfss://silver@azuredataengst.dfs.core.windows.net/DimArtist/data")
# MAGIC     .toTable("spotify_data.silver.DimArtist) """

# COMMAND ----------

df_silver_check_art = spark.read.format("delta").load("abfss://silver@azuredataengst.dfs.core.windows.net/DimArtist/data")
display(df_silver_check_art)

# COMMAND ----------

# MAGIC %md
# MAGIC **DimTrack**

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","abfss://silver@azuredataengst.dfs.core.windows.net/DimTrack/checkpoint")\
    .option("schemaEvolutionMode","addNewColumns")\
    .load("abfss://bronze@azuredataengst.dfs.core.windows.net/DimTrack")

# COMMAND ----------

#display(df_track, checkpointLocation = "abfss://silver@azuredataengst.dfs.core.windows.net/DimTrack/_checkpoint_display")

# COMMAND ----------

df_static_prev_track = spark.read.format("parquet").load("abfss://bronze@azuredataengst.dfs.core.windows.net/DimTrack")

# COMMAND ----------

display(df_static_prev_track)

# COMMAND ----------

display(df_track.columns)

# COMMAND ----------

df_track.printSchema()

# COMMAND ----------

df_static_prev_track = df_static_prev_track.withColumn("durationflag",when(col("duration_sec")<150,"low")\
    .when(col("duration_sec")<300,"medium")\
    .otherwise("high"))
display(df_static_prev_track)

# COMMAND ----------

df_track = df_track.withColumn("durationflag",when(col("duration_sec")<150,"low")\
    .when(col("duration_sec")<300,"medium")\
    .otherwise("high"))

# COMMAND ----------

display(df_track.columns)

# COMMAND ----------

df_static_prev_track = df_static_prev_track.withColumn("track_name",regexp_replace(col("track_name"),'-',' '))
display(df_static_prev_track)

# COMMAND ----------

df_track = df_track.withColumn("track_name",regexp_replace(col("track_name"),'-',' '))

# COMMAND ----------

df_track = reusable().dropColumns(df_track,['_rescued_data'])

# COMMAND ----------

df_track.writeStream.format("delta")\
.outputMode("append")\
.option("checkpointLocation","abfss://silver@azuredataengst.dfs.core.windows.net/DimTrack/checkpoint")\
.trigger(once=True)\
.option("path","abfss://silver@azuredataengst.dfs.core.windows.net/DimTrack/data")\
.toTable("spotify_data.silver.DimTrack")

# COMMAND ----------

df_silver_check_track = spark.read.format("delta").load("abfss://silver@azuredataengst.dfs.core.windows.net/DimTrack/data")
display(df_silver_check_track)

# COMMAND ----------

# MAGIC %md
# MAGIC **DimDate**

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","abfss://silver@azuredataengst.dfs.core.windows.net/DimDate/checkpoint")\
    .option("schemaEvolutionMode","addNewColumns")\
    .load("abfss://bronze@azuredataengst.dfs.core.windows.net/DimDate")

# COMMAND ----------

#display(df_date, checkpointLocation = "abfss://silver@azuredataengst.dfs.core.windows.net/DimDate/_checkpoint_display")

# COMMAND ----------

df_stat_prev_date = spark.read.format("parquet").load("abfss://bronze@azuredataengst.dfs.core.windows.net/DimDate")

# COMMAND ----------

display(df_stat_prev_date)

# COMMAND ----------

display(df_date.columns)

# COMMAND ----------

df_date.printSchema()

# COMMAND ----------

df_date = reusable().dropColumns(df_date,['_rescued_data'])

# COMMAND ----------

df_date.writeStream.format("delta")\
.outputMode("append")\
.option("checkpointLocation","abfss://silver@azuredataengst.dfs.core.windows.net/DimDate/checkpoint")\
.trigger(once=True)\
.option("path","abfss://silver@azuredataengst.dfs.core.windows.net/DimDate/data")\
.toTable("spotify_data.silver.DimDate")

# COMMAND ----------

df_silver_check_date = spark.read.format("delta").load("abfss://silver@azuredataengst.dfs.core.windows.net/DimDate/data")
display(df_silver_check_date)

# COMMAND ----------

# MAGIC %md
# MAGIC Remaining table creation

# COMMAND ----------

# MAGIC %md
# MAGIC **Fact Stream**

# COMMAND ----------

df_fact = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","abfss://silver@azuredataengst.dfs.core.windows.net/FactStream/checkpoint")\
    .option("schemaEvolutionMode","addNewColumns")\
    .load("abfss://bronze@azuredataengst.dfs.core.windows.net/FactStream")

# COMMAND ----------

#display(df_fact, ,checkpointLocation = "abfss://silver@azuredataengst.dfs.core.windows.net/FactStream/_checkpoint_display")

# COMMAND ----------

df_stat_prev_fact = spark.read.format("parquet").load("abfss://bronze@azuredataengst.dfs.core.windows.net/FactStream")
display(df_stat_prev_fact)

# COMMAND ----------

display(df_fact.columns)

# COMMAND ----------

df_fact.printSchema()

# COMMAND ----------

df_fact = reusable().dropColumns(df_fact,['_rescued_data'])

# COMMAND ----------

df_fact.writeStream.format("delta")\
.outputMode("append")\
.option("checkpointLocation","abfss://silver@azuredataengst.dfs.core.windows.net/FactStream/checkpoint")\
.trigger(once=True)\
.option("path","abfss://silver@azuredataengst.dfs.core.windows.net/FactStream/data")\
.toTable("spotify_data.silver.FactStream")

# COMMAND ----------

df_silver_check_fact = spark.read.format("delta").load("abfss://silver@azuredataengst.dfs.core.windows.net/DimDate/data")
display(df_silver_check_fact)