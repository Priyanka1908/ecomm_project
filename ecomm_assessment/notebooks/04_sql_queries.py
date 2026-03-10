# Databricks notebook source
# MAGIC %sql
# MAGIC -- Profit by year
# MAGIC SELECT
# MAGIC     year(order_date) AS order_year,
# MAGIC     round(sum(profit), 2) AS total_profit
# MAGIC FROM ecomm.silver.sales
# MAGIC GROUP BY year(order_date)
# MAGIC ORDER BY order_year;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Profit by year and product category
# MAGIC SELECT
# MAGIC     year(order_date) AS order_year,
# MAGIC     product_category,
# MAGIC     round(sum(profit), 2) AS total_profit
# MAGIC FROM ecomm.silver.sales
# MAGIC GROUP BY
# MAGIC     year(order_date),
# MAGIC     product_category
# MAGIC ORDER BY
# MAGIC     order_year;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Profit by customer (all time)
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     round(sum(profit), 2) AS total_profit
# MAGIC FROM ecomm.silver.sales
# MAGIC GROUP BY
# MAGIC     customer_id,
# MAGIC     customer_name
# MAGIC ORDER BY total_profit DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Profit by customer and year
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     year(order_date) AS order_year,
# MAGIC     round(sum(profit), 2) AS total_profit
# MAGIC FROM ecomm.silver.sales
# MAGIC GROUP BY
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     year(order_date)
# MAGIC ORDER BY
# MAGIC     customer_name,
# MAGIC     order_year;