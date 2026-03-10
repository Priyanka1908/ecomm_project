# E-Commerce Data Pipeline

This project processes raw e-commerce data using a medallion architecture on Databricks. The data comes from three source files — orders, products, and customers — and flows through Bronze, Silver, and Gold layers before being queried for analysis.


Project Structure

    ecomm_assessment/
    ├── config/
    │   └── config.yaml
    ├── notebooks/
    │   ├── 01_data_ingestion.py
    │   ├── 02_data_transformation.py
    │   ├── 03_data_aggregation.py
    │   └── 04_sql_queries.py
    │   └── 05_run_test.py
    └── tests/
        ├── conftest.py
        ├── test_ingestion.py
        ├── test_transformation.py
        └── test_aggregation.py


# Layers

1. Bronze — Raw Ingestion

Notebook 01_data_ingestion.py reads the three source files from a Databricks volume and writes them into Delta tables under ecomm.bronze. The only changes at this stage are:

- Adding an ingestion_timestamp column to track when data was loaded
- Cleaning column names to remove spaces and special characters (Delta doesn't support them)

Tables created: ecomm.bronze.orders, ecomm.bronze.products, ecomm.bronze.customer


2. Silver — Cleaned and Enriched

Notebook 02_data_transformation.py reads from Bronze and applies data quality fixes.

Orders:
- Null customer_id and product_id replaced with UNKNOWN_CUST / UNKNOWN_PROD
- Dates parsed from raw string to date type
- Discounts outside 0–1 range are nulled out
- Zero or negative quantities are flagged as null

Customers:
- Null names defaulted to UNKNOWN
- Emails validated against a regex, invalid ones replaced with "Invalid"
- Phone numbers stripped of formatting, flagged as "Invalid" if too short
- Dirty name values like numbers and special characters are removed

Products:
- Price cleaned to numeric
- Duplicate product IDs removed

After all three are cleaned, they are joined into a single silver_sales table combining order, customer, and product details. This is the main table used for analysis.

An OPTIMIZE ZORDER BY (customer_id, order_date) is run on silver.sales after the write to improve read performance.

Tables created: ecomm.silver.customer, ecomm.silver.products, ecomm.silver.sales


3. Gold — Aggregated

Notebook 03_data_aggregation.py reads from silver.sales and computes total profit per customer, per product sub-category, per year. The result is written to ecomm.gold.profit_aggregation, partitioned by order_year.

Table created: ecomm.gold.profit_aggregation


4. SQL Queries

Notebook 04_sql_queries.py has four analytical queries run on silver.sales:

- Total profit by year
- Total profit by year and product category
- Total profit by customer (all time)
- Total profit by customer and year


Configuration

All table names, file paths, and validation thresholds are in config/config.yaml. Nothing is hardcoded in the notebooks. To point the pipeline at a different catalog or volume, only the config file needs to change.


Tests

Tests are in the tests/ folder and run locally using pytest with a local Spark session — no Databricks connection needed.

To run:

    pip install pyspark pytest
    pytest tests/

The tests cover ingestion utilities, all transformation rules (null handling, email and phone validation, discount and quantity checks, date parsing, joins), and the gold aggregation logic.

Note: There are assumptions as per the data, which can be discussed during our discussion.
