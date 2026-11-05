# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
"""
00 - Workspace Sanity Check

Verify compute access, Unity Catalog permissions, and required libraries before starting the course modules.
"""

# TODO: implement this module


# COMMAND ----------

# DBTITLE 1,Cell 1: Verify runtime configuration, memory, and Spark version
# Cell 1: Environment diagnostics and partition optimization
import sys
import os
import pyspark

print(f"Python Kernel: {sys.version}")
print(f"PySpark Version: {pyspark.__version__}")
try:
    app_id = spark.sparkContext.applicationId
except Exception:
    # sparkContext and spark.app.id are not exposed on serverless compute
    app_id = "N/A (not exposed on serverless compute)"
print(f"Active Spark App ID: {app_id}")
print(f"Current Working Directory: {os.getcwd()}")

# Optimize shuffle partitions for Community Edition (2 vCPUs)
# Avoids default 200 tasks which causes JVM memory thrashing
spark.conf.set("spark.sql.shuffle.partitions", "4")
print(f"Configured spark.sql.shuffle.partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# COMMAND ----------

# Cell 2: Test file I/O adhering strictly to workspace security rules
import os
import shutil

# Resolve scratch path within the permitted /Workspace tree
current_dir = os.getcwd()
test_dir = os.path.join(current_dir, ".workspace_io_test")
uri_path = f"file:{test_dir}"

# 1. Clean scratch directory
if os.path.exists(test_dir):
    shutil.rmtree(test_dir)
os.makedirs(test_dir, exist_ok=True)

# 2. Write verification payload
heartbeat_file = os.path.join(test_dir, "cluster_heartbeat.txt")
payload_text = "Databricks Community Edition: Workspace storage isolation verified."
with open(heartbeat_file, "w") as f:
    f.write(payload_text)

# 3. Read back via dbutils using explicit file: scheme
files = dbutils.fs.ls(uri_path)
for file_meta in files:
    print(f"Found: {file_meta.name} | Size: {file_meta.size} bytes | Path: {file_meta.path}")

read_data = dbutils.fs.head(f"{uri_path}/cluster_heartbeat.txt")
print(f"\nRead Payload Output: {read_data}")

# 4. Clean up scratch directory
shutil.rmtree(test_dir)
print("Scratch test directory successfully removed.")

# COMMAND ----------

# Cell 3: Verify managed Delta table creation without explicit paths
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

schema = StructType([
    StructField("component", StringType(), False),
    StructField("http_status", IntegerType(), False),
    StructField("validated_at", TimestampType(), False)
])

data = [
    ("SparkDriverMaster", 200, datetime.utcnow()),
    ("WorkspaceFilesystem", 200, datetime.utcnow()),
    ("MetastoreCatalog", 200, datetime.utcnow())
]

df_sanity = spark.createDataFrame(data, schema)

# Write to managed Delta table (storage handled automatically by metastore)
df_sanity.write.format("delta").mode("overwrite").saveAsTable("module01_sanity_log")
print("Successfully registered managed Delta table: module01_sanity_log")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select component, http_status, validated_at
# MAGIC from module01_sanity_log