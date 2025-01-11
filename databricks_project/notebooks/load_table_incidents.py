# Databricks notebook source
import os
import requests

# URL of the JSON file
data_url = "https://raw.githubusercontent.com/adolph/getIncidentsGit/main/data.json"

# Define local path for saving the JSON file
local_dir = "/dbfs/tmp/"
local_path = os.path.join(local_dir, "incidents.json")

# Ensure the directory exists
os.makedirs(local_dir, exist_ok=True)

# Download the JSON file
response = requests.get(data_url)
response.raise_for_status()  # Ensure the request was successful

# Save the file
with open(local_path, "w") as file:
    file.write(response.text)

print(f"File downloaded successfully to {local_path}.")


# COMMAND ----------

# Load the JSON file into Spark
df = spark.read.option("multiline", "true").json("dbfs:/tmp/incidents.json")

# Extract the required fields
features = df.selectExpr("explode(features) as feature").select("feature.attributes.*")

# Define required columns
required_columns = ["UID", "Agency", "Address", "CrossStreet", "CALL_TIME", "CombinedResponse"]

# Select only the required columns
filtered_df = features.select(*required_columns)

# Show the transformed data
filtered_df.show(truncate=False)


# COMMAND ----------

display(filtered_df.count())

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Path to the Delta table
delta_table_path = "abfss://bamboo@extstorageusestag.dfs.core.windows.net/bronze/incidents_bronze"


# Deduplicate the source DataFrame (filtered_df) based on UID
window_spec = Window.partitionBy("UID").orderBy("CALL_TIME")
deduplicated_df = filtered_df.withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

# Check if the Delta table exists
if DeltaTable.isDeltaTable(spark, delta_table_path):
    # If the Delta table exists, perform an upsert (MERGE)
    delta_table = DeltaTable.forPath(spark, delta_table_path)
    delta_table.alias("target").merge(
        deduplicated_df.alias("source"),
        "target.UID = source.UID"
    ).whenMatchedUpdate(set={
        "Agency": col("source.Agency"),
        "Address": col("source.Address"),
        "CrossStreet": col("source.CrossStreet"),
        "CALL_TIME": col("source.CALL_TIME"),
        "CombinedResponse": col("source.CombinedResponse")
    }).whenNotMatchedInsert(values={
        "UID": col("source.UID"),
        "Agency": col("source.Agency"),
        "Address": col("source.Address"),
        "CrossStreet": col("source.CrossStreet"),
        "CALL_TIME": col("source.CALL_TIME"),
        "CombinedResponse": col("source.CombinedResponse")
    }).execute()
else:
    # If the Delta table does not exist, use saveAsTable to create it and register in the metastore
    deduplicated_df.write \
        .format("delta") \
        .option("path", delta_table_path) \
        .saveAsTable("houstonactiveincidents.bronze.incidents")
