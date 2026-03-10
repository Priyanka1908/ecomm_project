# Databricks notebook source
import yaml
from pyspark.sql.functions import (
    broadcast, col, coalesce, current_timestamp, length, lit,
    regexp_replace, round, to_date, trim, when,
)
from pyspark.sql.types import StringType

spark.conf.set("spark.sql.shuffle.partitions", "auto")

with open("../config/config.yaml", "r") as f:
    cfg = yaml.safe_load(f)

bronze     = cfg["tables"]["bronze"]
silver     = cfg["tables"]["silver"]
date_fmt   = cfg["validation"]["date_format"]
disc_min   = cfg["validation"]["discount_min"]
disc_max   = cfg["validation"]["discount_max"]
qty_min    = cfg["validation"]["quantity_min"]
phone_len  = cfg["validation"]["phone_min_length"]

# COMMAND ----------

orders_bronze   = spark.table(bronze["orders"])
products_bronze = spark.table(bronze["products"])
customer_bronze = spark.table(bronze["customer"])

# COMMAND ----------

def trim_string_columns(df):
    return df.select([
        trim(col(c)).alias(c) if isinstance(df.schema[c].dataType, StringType) else col(c)
        for c in df.columns
    ])

# COMMAND ----------

orders_cleaned = orders_bronze \
    .withColumn("customer_id", coalesce(col("customer_id"), lit("UNKNOWN_CUST"))) \
    .withColumn("product_id", coalesce(col("product_id"), lit("UNKNOWN_PROD"))) \
    .dropDuplicates(["row_id"]) \
    .withColumn("order_date", to_date(col("order_date"), date_fmt)) \
    .withColumn("ship_date", to_date(col("ship_date"), date_fmt)) \
    .withColumn(
        "discount",
        when((col("discount") < disc_min) | (col("discount") > disc_max), None)
        .otherwise(col("discount"))
    ) \
    .withColumn(
        "quantity",
        when(col("quantity") < qty_min, None)
        .otherwise(col("quantity"))
    ) \
    .withColumn("profit", round(col("profit"), 2)) \
    .withColumn("silver_processing_timestamp", current_timestamp())

orders_cleaned = trim_string_columns(orders_cleaned)

# COMMAND ----------

silver_customer = customer_bronze \
    .withColumn("customer_id", coalesce(col("customer_id"), lit("UNKNOWN_CUST"))) \
    .withColumn("customer_name", coalesce(col("customer_name"), lit("UNKNOWN"))) \
    .withColumn(
        "email",
        when(
            col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"),
            col("email")
        ).otherwise("Invalid")
    ) \
    .withColumn(
        "customer_name",
        regexp_replace(
            regexp_replace(col("customer_name"), "[^a-zA-Z ]", ""),
            "\\s+", " "
        )
    ) \
    .withColumn("phone", regexp_replace(col("phone").cast(StringType()), "[^0-9]", "")) \
    .withColumn(
        "phone",
        when(length(col("phone")) >= phone_len, col("phone")).otherwise("Invalid")
    ) \
    .dropDuplicates(["customer_id"]) \
    .withColumn("silver_processing_timestamp", current_timestamp())

silver_customer = trim_string_columns(silver_customer)

# COMMAND ----------

silver_products = products_bronze \
    .withColumn("product_id", coalesce(col("product_id"), lit("UNKNOWN_PROD"))) \
    .withColumn(
        "price_per_product",
        regexp_replace(col("price_per_product"), "[^0-9.]", "")
    ) \
    .withColumn(
        "price_per_product",
        when(col("price_per_product") == "", None)
        .otherwise(col("price_per_product").cast("double"))
    ) \
    .withColumn(
        "state",
        regexp_replace(
            regexp_replace(col("state"), "[^a-zA-Z ]", ""),
            "\\s+", " "
        )
    ) \
    .dropDuplicates(["product_id"]) \
    .withColumn("silver_processing_timestamp", current_timestamp())

silver_products = trim_string_columns(silver_products)

# COMMAND ----------

silver_sales = orders_cleaned.alias("o") \
    .join(broadcast(silver_customer).alias("c"), on="customer_id", how="left") \
    .join(broadcast(silver_products).alias("p"), on="product_id", how="left") \
    .select(
        col("o.*"),
        col("c.customer_name"),
        col("c.country").alias("customer_country"),
        col("p.category").alias("product_category"),
        col("p.sub_category").alias("product_sub_category")
    )

silver_sales = trim_string_columns(silver_sales)

# COMMAND ----------

for df, name, table in [
    (silver_customer, "silver_customer", silver["customer"]),
    (silver_products, "silver_products", silver["products"]),
    (silver_sales,    "silver_sales",    silver["sales"]),
]:
    if df.count() == 0:
        raise Exception(f"{name} is empty,aborting write to {table}")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"{table}: {df.count()} rows")

# COMMAND ----------

spark.sql(f"OPTIMIZE {silver['sales']} ZORDER BY (customer_id, order_date)")

# COMMAND ----------

display(spark.table(silver["customer"]))

# COMMAND ----------

display(spark.table(silver["products"]))

# COMMAND ----------

display(spark.table(silver["sales"]))