# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
"""
02 - Schema Transformation

Apply an explicit schema and transform raw batch data into a clean, typed DataFrame for the silver layer.
"""

# TODO: implement this module

# COMMAND ----------

# Cell 1: Parse nested GeoJSON features array using an explicit StructType
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, \
    LongType, IntegerType, ArrayType
)


df_bronze = spark.table("bronze_earthquake_raw")

# Schema for the inner 'properties' object
properties_schema = StructType([
    StructField("mag", DoubleType(), True),
    StructField("place", StringType(), True),
    StructField("time", LongType(), True),        # Epoch millisecond timestamp
    StructField("updated", LongType(), True),
    StructField("url", StringType(), True),
    StructField("alert", StringType(), True),
    StructField("status", StringType(), True),
    StructField("tsunami", IntegerType(), True),
    StructField("sig", IntegerType(), True),
    StructField("type", StringType(), True)
])

# Schema for the inner 'geometry' object (coordinates: [lon, lat, depth])
geometry_schema = StructType([
    StructField("type", StringType(), True),
    StructField("coordinates", ArrayType(DoubleType()), True)
])

# Schema for each item in the 'features' array
feature_item_schema = StructType([
    StructField("id", StringType(), True),
    StructField("properties", properties_schema, True),
    StructField("geometry", geometry_schema, True)
])

# Top-level GeoJSON schema
root_schema = StructType([
    StructField("type", StringType(), True),
    StructField("features", ArrayType(feature_item_schema), True)
])

# Parse root JSON string
df_parsed = df_bronze.withColumn("parsed", from_json(col("raw_payload"), root_schema))
print("Successfully compiled GeoJSON schema parser.")

# COMMAND ----------

# Cell 2: Explode features array and flatten nested structs into Silver Delta Table
from pyspark.sql.functions import explode, to_timestamp

# Explode the array of features into separate rows
df_exploded = (
    df_parsed
    .select(explode(col("parsed.features")).alias("feature"), col("ingest_timestamp"))
)

# Extract nested coordinates and properties, cast epoch milliseconds to timestamp
df_silver = df_exploded.select(
    col("feature.id").alias("earthquake_id"),
    col("feature.properties.place").alias("location_desc"),
    col("feature.properties.mag").alias("magnitude"),
    col("feature.properties.tsunami").alias("tsunami_flag"),
    col("feature.properties.sig").alias("significance_score"),
    col("feature.properties.status").alias("event_status"),
    # Convert epoch millis to UTC Timestamp
    to_timestamp(col("feature.properties.time") / 1000).alias("event_timestamp"),
    # Array indices: 0 = Longitude, 1 = Latitude, 2 = Depth (km)
    col("feature.geometry.coordinates")[0].alias("longitude"),
    col("feature.geometry.coordinates")[1].alias("latitude"),
    col("feature.geometry.coordinates")[2].alias("depth_km"),
    col("ingest_timestamp")
).filter(col("earthquake_id").isNotNull() & col("magnitude").isNotNull())

# Write to managed Silver Delta table
df_silver.write.format("delta").mode("overwrite").saveAsTable("silver_earthquake_events")

row_count = df_silver.count()
print(f"Successfully processed and written {row_count} records into silver_earthquake_events.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cell 3: Aggregate global seismic impact by date and severity categories
# MAGIC CREATE OR REPLACE TABLE gold_earthquake_daily_summary AS
# MAGIC SELECT 
# MAGIC     to_date(event_timestamp) AS event_date,
# MAGIC     COUNT(1) AS total_earthquakes,
# MAGIC     ROUND(AVG(magnitude), 2) AS avg_magnitude,
# MAGIC     ROUND(MAX(magnitude), 2) AS max_magnitude,
# MAGIC     ROUND(AVG(depth_km), 2) AS avg_depth_km,
# MAGIC     SUM(CASE WHEN tsunami_flag = 1 THEN 1 ELSE 0 END) AS tsunami_warning_events,
# MAGIC     SUM(CASE WHEN magnitude >= 4.5 THEN 1 ELSE 0 END) AS moderate_to_severe_count
# MAGIC FROM silver_earthquake_events
# MAGIC GROUP BY to_date(event_timestamp)
# MAGIC ORDER BY event_date DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Display the latest 10 days of aggregated metrics
# MAGIC SELECT * FROM gold_earthquake_daily_summary LIMIT 10;