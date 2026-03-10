from datetime import date

from pyspark.sql.functions import round, sum, year


def test_gold_profit_has_expected_columns(spark):
    sales = spark.createDataFrame(
        [("C1", "Alice", "Furniture", "Chairs", date(2016, 8, 21), 63.69)],
        ["customer_id", "customer_name", "product_category", "product_sub_category", "order_date", "profit"]
    )
    result = sales \
        .withColumn("order_year", year("order_date")) \
        .groupBy("order_year", "product_category", "product_sub_category", "customer_id", "customer_name") \
        .agg(round(sum("profit"), 2).alias("total_profit"))

    assert "order_year" in result.columns
    assert "total_profit" in result.columns
    assert "customer_id" in result.columns
    assert "product_category" in result.columns


def test_gold_profit_aggregation_sums_correctly(spark):
    sales = spark.createDataFrame(
        [
            ("C1", "Alice", "Furniture", "Chairs", date(2016, 8, 21), 63.69),
            ("C1", "Alice", "Furniture", "Chairs", date(2016, 9, 5), 100.0),
        ],
        ["customer_id", "customer_name", "product_category", "product_sub_category", "order_date", "profit"]
    )
    result = sales \
        .withColumn("order_year", year("order_date")) \
        .groupBy("order_year", "product_category", "product_sub_category", "customer_id", "customer_name") \
        .agg(round(sum("profit"), 2).alias("total_profit"))

    assert result.collect()[0]["total_profit"] == 163.69


def test_gold_profit_rounded_to_2_decimals(spark):
    sales = spark.createDataFrame(
        [("C1", "Alice", "Technology", "Phones", date(2017, 1, 1), 10.1234)],
        ["customer_id", "customer_name", "product_category", "product_sub_category", "order_date", "profit"]
    )
    result = sales \
        .withColumn("order_year", year("order_date")) \
        .groupBy("order_year", "product_category", "product_sub_category", "customer_id", "customer_name") \
        .agg(round(sum("profit"), 2).alias("total_profit"))

    assert result.collect()[0]["total_profit"] == 10.12


def test_gold_profit_groups_by_year(spark):
    sales = spark.createDataFrame(
        [
            ("C1", "Alice", "Furniture", "Chairs", date(2016, 1, 1), 50.0),
            ("C1", "Alice", "Furniture", "Chairs", date(2017, 1, 1), 75.0),
        ],
        ["customer_id", "customer_name", "product_category", "product_sub_category", "order_date", "profit"]
    )
    result = sales \
        .withColumn("order_year", year("order_date")) \
        .groupBy("order_year", "product_category", "product_sub_category", "customer_id", "customer_name") \
        .agg(round(sum("profit"), 2).alias("total_profit"))

    assert result.count() == 2
