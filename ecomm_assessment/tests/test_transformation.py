from pyspark.sql.functions import (
    col, coalesce, length, lit,
    regexp_replace, to_date, when,
)
from pyspark.sql.types import LongType, StringType, StructField, StructType

CFG = {
    "validation": {
        "date_format": "d/M/yyyy",
        "discount_min": 0,
        "discount_max": 1,
        "quantity_min": 1,
        "phone_min_length": 8,
    }
}


def test_null_customer_name_filled(spark):
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("customer_name", StringType()),
    ])
    df = spark.createDataFrame([("C1", None), ("C2", "John")], schema)
    result = df.withColumn("customer_name", coalesce(col("customer_name"), lit("UNKNOWN")))
    assert result.filter(col("customer_name") == "UNKNOWN").count() == 1


def test_phone_negative_integer_flagged_invalid(spark):
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("phone", LongType()),
    ])
    df = spark.createDataFrame([("C1", -6181), ("C2", 7185624866)], schema)
    result = df \
        .withColumn("phone", regexp_replace(col("phone").cast(StringType()), "[^0-9]", "")) \
        .withColumn("phone", when(length(col("phone")) >= 8, col("phone")).otherwise("Invalid"))
    assert result.filter(col("phone") == "Invalid").count() == 1


def test_invalid_email_flagged(spark):
    df = spark.createDataFrame(
        [("C1", "not-an-email"), ("C2", "valid@email.com")],
        ["customer_id", "email"]
    )
    result = df.withColumn(
        "email",
        when(
            col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"),
            col("email")
        ).otherwise("Invalid")
    )
    assert result.filter(col("email") == "Invalid").count() == 1


def test_invalid_discount_nulled(spark):
    df = spark.createDataFrame(
        [(1, "C1", "P1", 0.3), (2, "C2", "P2", -0.1), (3, "C3", "P3", 1.5)],
        ["row_id", "customer_id", "product_id", "discount"]
    )
    disc_min = CFG["validation"]["discount_min"]
    disc_max = CFG["validation"]["discount_max"]
    result = df.withColumn(
        "discount",
        when((col("discount") < disc_min) | (col("discount") > disc_max), None)
        .otherwise(col("discount"))
    )
    assert result.filter(col("discount").isNull()).count() == 2


def test_invalid_quantity_nulled(spark):
    df = spark.createDataFrame(
        [(1, "C1", "P1", 5), (2, "C2", "P2", 0), (3, "C3", "P3", -1)],
        ["row_id", "customer_id", "product_id", "quantity"]
    )
    qty_min = CFG["validation"]["quantity_min"]
    result = df.withColumn(
        "quantity",
        when(col("quantity") < qty_min, None).otherwise(col("quantity"))
    )
    assert result.filter(col("quantity").isNull()).count() == 2


def test_order_date_parsed(spark):
    df = spark.createDataFrame(
        [(1, "21/8/2016"), (2, "6/10/2016")],
        ["row_id", "order_date"]
    )
    result = df.withColumn("order_date", to_date(col("order_date"), CFG["validation"]["date_format"]))
    assert result.filter(col("order_date").isNull()).count() == 0


def test_silver_sales_has_enriched_columns(spark):
    orders = spark.createDataFrame(
        [(1, "C1", "P1", 0.1, 2, 50.0, 10.0)],
        ["row_id", "customer_id", "product_id", "discount", "quantity", "price", "profit"]
    )
    customers = spark.createDataFrame(
        [("C1", "Alice", "United States")],
        ["customer_id", "customer_name", "country"]
    )
    products = spark.createDataFrame(
        [("P1", "Furniture", "Chairs")],
        ["product_id", "category", "sub_category"]
    )
    result = orders.alias("o") \
        .join(customers.alias("c"), on="customer_id", how="left") \
        .join(products.alias("p"), on="product_id", how="left") \
        .select(
            col("o.*"),
            col("c.customer_name"),
            col("c.country").alias("customer_country"),
            col("p.category").alias("product_category"),
            col("p.sub_category").alias("product_sub_category")
        )
    assert "customer_name" in result.columns
    assert "customer_country" in result.columns
    assert "product_category" in result.columns
    assert "product_sub_category" in result.columns
