# Databricks notebook source


from pyspark.sql import SparkSession

# Create or get the SparkSession
spark = SparkSession.builder.getOrCreate()

# Define the path to the CSV file
csv_file_path = "dbfs:/FileStore/shared_uploads/mixalis.vardakis@gmail.com/Position_Salaries.csv"

# Read the CSV file into a DataFrame
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Specify the Delta table path where you want to import the data
delta_table_path = "salaries_table"

# Write the DataFrame to the Delta table
df.write.format("delta").mode("overwrite").saveAsTable(delta_table_path)

# COMMAND ----------

import pandas as pd

df1 = pd.read_csv("/dbfs/FileStore/shared_uploads/mixalis.vardakis@gmail.com/Data.csv")

# COMMAND ----------



from pyspark.sql import SparkSession

# Create or get the SparkSession
spark = SparkSession.builder.getOrCreate()

# Define the path to the CSV file
csv_file_path = "dbfs:/FileStore/shared_uploads/mixalis.vardakis@gmail.com/Data.csv"

# Read the CSV file into a DataFrame
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Specify the Delta table path where you want to import the data
delta_table_path = "power_plants"

# Write the DataFrame to the Delta table
df.write.format("delta").mode("overwrite").saveAsTable(delta_table_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from power_plants
