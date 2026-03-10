# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

import re

import yaml
from pyspark.sql.functions import current_timestamp

with open("../config/config.yaml", "r") as f:
    cfg = yaml.safe_load(f)

spark.conf.set("spark.sql.shuffle.partitions", "auto")

raw_path = cfg["paths"]["raw"]
files    = cfg["paths"]["files"]
bronze   = cfg["tables"]["bronze"]

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ecomm;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecomm.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecomm.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecomm.gold;

# COMMAND ----------

display(dbutils.fs.ls(raw_path))

# COMMAND ----------

import pandas as pd
try:
    orders_df   = spark.read.option("multiline", True).json(raw_path + files["orders"])
    products_df = spark.read.option("header", True).option("inferSchema", True).csv(raw_path + files["products"])
    #customer_df = spark.read.option("headerRows", 1).option("inferSchema","true").excel(raw_path + files["customer"])
    customer_pd = pd.read_excel(raw_path + files["customer"])
    customer_pd = customer_pd.astype({c: str for c in customer_pd.select_dtypes(include="object").columns})
    customer_df = spark.createDataFrame(customer_pd)
except Exception as e:
    raise Exception(f"Failed to read source files from {raw_path}: {e}")

# COMMAND ----------

def add_ingestion_ts(df):
    return df.withColumn("ingestion_timestamp", current_timestamp())

orders_df   = add_ingestion_ts(orders_df)
products_df = add_ingestion_ts(products_df)
customer_df = add_ingestion_ts(customer_df)

# COMMAND ----------

def clean_columns(df):
    return df.toDF(*[re.sub(r"\W+", "_", c.lower()) for c in df.columns])

orders_df   = clean_columns(orders_df)
products_df = clean_columns(products_df)
customer_df = clean_columns(customer_df)

# COMMAND ----------

orders_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(bronze["orders"])
products_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(bronze["products"])
customer_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(bronze["customer"])

for name, table in bronze.items():
    count = spark.table(table).count()
    if count == 0:
        raise Exception(f"Bronze table {table} is empty after write")
    print(f"{table}: {count} rows")

# COMMAND ----------

display(spark.table(bronze["orders"]))

# COMMAND ----------

display(spark.table(bronze["products"]))

# COMMAND ----------

display(spark.table(bronze["customer"]))