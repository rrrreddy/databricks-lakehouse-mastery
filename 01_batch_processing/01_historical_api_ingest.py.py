# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
"""
01 - Historical API Ingest (Batch)

Pull historical data from a public REST API in batch and land it as raw files in a Unity Catalog volume.
"""

# TODO: implement this module

# COMMAND ----------

# DBTITLE 1,Cell 2
# Cell 1: Optimize Spark runtime parameters for Databricks Community Edition
spark.conf.set("spark.sql.shuffle.partitions", 4)

print(f"Spark runtime parameters optimized: {spark.conf.get('spark.sql.shuffle.partitions')}")

# COMMAND ----------

# DBTITLE 1,Cell 3
# Cell 2: Fetch unmetered public open data from USGS
import requests
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Free public feed: All earthquakes magnitude 1.0+ over the past 30 days
api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_month.geojson"

response = requests.get(api_url, timeout=30)
response.raise_for_status()
raw_json_str = response.text

# Wrap payload in a Bronze audit record
bronze_record = [{
    "source_endpoint": "https://earthquake.usgs.gov",
    "feed_name": "all_month_earthquakes",
    "raw_payload": raw_json_str,
    "ingest_timestamp": datetime.utcnow()
}]

bronze_schema = StructType([
    StructField("source_endpoint", StringType(), False),
    StructField("feed_name", StringType(), False),
    StructField("raw_payload", StringType(), False),
    StructField("ingest_timestamp", TimestampType(), False)
])

# Write raw payload to managed Bronze Delta table
df_raw = spark.createDataFrame(bronze_record, bronze_schema)
df_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_earthquake_raw")

print("Successfully wrote raw GeoJSON payload to table: bronze_earthquake_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     source_endpoint, 
# MAGIC     feed_name, 
# MAGIC     length(raw_payload) AS payload_bytes, 
# MAGIC     ingest_timestamp 
# MAGIC FROM bronze_earthquake_raw;