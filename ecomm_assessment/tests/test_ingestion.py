import re

from pyspark.sql.functions import current_timestamp


def add_ingestion_ts(df):
    return df.withColumn("ingestion_timestamp", current_timestamp())


def clean_columns(df):
    return df.toDF(*[re.sub(r"\W+", "_", c.lower()) for c in df.columns])


def test_add_ingestion_ts_adds_column(spark):
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])
    result = add_ingestion_ts(df)
    assert "ingestion_timestamp" in result.columns


def test_add_ingestion_ts_no_nulls(spark):
    df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    result = add_ingestion_ts(df)
    assert result.filter(result.ingestion_timestamp.isNull()).count() == 0


def test_clean_columns_removes_spaces(spark):
    df = spark.createDataFrame([("C1", "2024-01-01")], ["Customer ID", "Order Date"])
    result = clean_columns(df)
    assert "customer_id" in result.columns
    assert "order_date" in result.columns


def test_clean_columns_lowercase(spark):
    df = spark.createDataFrame([("P1", "Furniture")], ["ProductID", "CATEGORY"])
    result = clean_columns(df)
    for col_name in result.columns:
        assert col_name == col_name.lower()


def test_clean_columns_removes_special_chars(spark):
    df = spark.createDataFrame([("x",)], ["Price per product"])
    result = clean_columns(df)
    assert "price_per_product" in result.columns
