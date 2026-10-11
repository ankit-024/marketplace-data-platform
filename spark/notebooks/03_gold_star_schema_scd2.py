# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Gold Layer: Star Schema Modeling & SCD Type 2 History
# MAGIC 
# MAGIC **Marketplace Intelligence Platform**  
# MAGIC Builds business-facing analytical models:
# MAGIC 1. **Date Dimension (`dim_date`)**
# MAGIC 2. **Slowly Changing Dimensions (SCD Type 2)** for `dim_seller` and `dim_product`
# MAGIC 3. **Fact Orders (`fact_orders`)** with Surrogate Keys (`_sk`)
# MAGIC 4. **Business Analytics Marts** (`gold_seller_daily_performance`)

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS marketplace_gold;

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1. Date Dimension (`dim_date`)
# MAGIC Generates a calendar dimension table for time-based filtering and slicing.

# COMMAND ----------
from pyspark.sql.functions import col, explode, sequence, to_date, lit, year, month, quarter, dayofmonth, date_format, dayofweek, when, sha2, concat_ws, monotonically_increasing_id

spark.sql("""
    CREATE OR REPLACE TABLE marketplace_gold.dim_date AS
    WITH date_range AS (
        SELECT explode(sequence(to_date('2026-01-01'), to_date('2026-12-31'), interval 1 day)) AS full_date
    )
    SELECT
        cast(date_format(full_date, 'yyyyMMdd') AS INT) AS date_sk,
        full_date,
        dayofmonth(full_date) AS day_of_month,
        date_format(full_date, 'EEEE') AS day_name,
        month(full_date) AS month,
        date_format(full_date, 'MMMM') AS month_name,
        quarter(full_date) AS quarter,
        year(full_date) AS year,
        CASE WHEN dayofweek(full_date) IN (1, 7) THEN true ELSE false END AS is_weekend
    FROM date_range;
""")

print(f"Generated 'dim_date': {spark.table('marketplace_gold.dim_date').count():,} calendar days")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2. Customer Dimension (`dim_customer`)

# COMMAND ----------
df_customers_silver = spark.table("marketplace_silver.customers_clean")

df_dim_customer = df_customers_silver \
    .select(
        monotonically_increasing_id().alias("customer_sk"),
        col("customer_id"),
        col("full_name"),
        col("email"),
        col("city"),
        col("state"),
        col("segment"),
        col("signup_ts")
    )

df_dim_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marketplace_gold.dim_customer")

print(f"Saved 'dim_customer': {spark.table('marketplace_gold.dim_customer').count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3. Slowly Changing Dimension Type 2 (`dim_seller`)
# MAGIC Implements SCD Type 2 versioning:
# MAGIC - `valid_from`: Start date of attribute validity
# MAGIC - `valid_to`: End date (`9999-12-31` for current active record)
# MAGIC - `is_current`: Boolean flag (`true`/`false`)
# MAGIC - `seller_sk`: Unique surrogate key for each historical version!

# COMMAND ----------
df_sellers_silver = spark.table("marketplace_silver.sellers_clean")

# Initial population / SCD2 Baseline
df_dim_seller_initial = df_sellers_silver \
    .select(
        monotonically_increasing_id().alias("seller_sk"),
        col("seller_id"),
        col("seller_name"),
        col("category"),
        col("city"),
        col("state"),
        col("rating"),
        col("registration_ts").alias("valid_from"),
        to_date(lit("9999-12-31")).alias("valid_to"),
        lit(True).alias("is_current")
    )

df_dim_seller_initial.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marketplace_gold.dim_seller")

print(f"Saved 'dim_seller' (SCD2 Baseline): {spark.table('marketplace_gold.dim_seller').count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4. Product Dimension with SCD Type 2 (`dim_product`)

# COMMAND ----------
df_products_silver = spark.table("marketplace_silver.products_clean")

df_dim_product_initial = df_products_silver \
    .select(
        monotonically_increasing_id().alias("product_sk"),
        col("product_id"),
        col("seller_id"),
        col("product_name"),
        col("category"),
        col("sub_category"),
        col("price"),
        col("cost"),
        col("created_ts").alias("valid_from"),
        to_date(lit("9999-12-31")).alias("valid_to"),
        lit(True).alias("is_current")
    )

df_dim_product_initial.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marketplace_gold.dim_product")

print(f"Saved 'dim_product' (SCD2 Baseline): {spark.table('marketplace_gold.dim_product').count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5. Fact Orders (`fact_orders` - Star Schema)
# MAGIC Joins the cleaned orders stream with active dimension versions to resolve Surrogate Keys:
# MAGIC - `customer_sk`
# MAGIC - `seller_sk`
# MAGIC - `product_sk`
# MAGIC - `date_sk`

# COMMAND ----------
spark.sql("""
    CREATE OR REPLACE TABLE marketplace_gold.fact_orders AS
    SELECT
        o.order_id,
        c.customer_sk,
        s.seller_sk,
        p.product_sk,
        d.date_sk,
        o.order_ts,
        o.quantity,
        o.unit_price,
        o.order_amount,
        o.status,
        o.order_date
    FROM marketplace_silver.orders_clean o
    LEFT JOIN marketplace_gold.dim_customer c 
        ON o.customer_id = c.customer_id
    LEFT JOIN marketplace_gold.dim_seller s 
        ON o.seller_id = s.seller_id AND s.is_current = true
    LEFT JOIN marketplace_gold.dim_product p 
        ON o.product_id = p.product_id AND p.is_current = true
    LEFT JOIN marketplace_gold.dim_date d 
        ON o.order_date = d.full_date;
""")

print(f"Built 'fact_orders': {spark.table('marketplace_gold.fact_orders').count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 6. Analytical Mart: Daily Seller Performance
# MAGIC Answers core business questions:
# MAGIC - Total GMV generated per seller per day
# MAGIC - Completed vs Cancelled order counts
# MAGIC - Average Order Value (AOV)

# COMMAND ----------
spark.sql("""
    CREATE OR REPLACE TABLE marketplace_gold.mart_daily_seller_performance AS
    SELECT
        s.seller_id,
        s.seller_name,
        s.category,
        s.city,
        s.state,
        f.order_date,
        COUNT(f.order_id) AS total_orders,
        SUM(CASE WHEN f.status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_orders,
        SUM(CASE WHEN f.status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_orders,
        ROUND(SUM(CASE WHEN f.status = 'COMPLETED' THEN f.order_amount ELSE 0 END), 2) AS total_gmv,
        ROUND(AVG(f.order_amount), 2) AS avg_order_value,
        ROUND(100.0 * SUM(CASE WHEN f.status = 'COMPLETED' THEN 1 ELSE 0 END) / COUNT(f.order_id), 1) AS completion_rate_pct
    FROM marketplace_gold.fact_orders f
    JOIN marketplace_gold.dim_seller s ON f.seller_sk = s.seller_sk
    GROUP BY
        s.seller_id,
        s.seller_name,
        s.category,
        s.city,
        s.state,
        f.order_date
    ORDER BY total_gmv DESC;
""")

print(f"Created 'mart_daily_seller_performance': {spark.table('marketplace_gold.mart_daily_seller_performance').count():,} aggregated rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 7. Quick Business Insights Verification

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Top 5 Sellers by Total GMV
# MAGIC SELECT 
# MAGIC     seller_name, 
# MAGIC     category, 
# MAGIC     city, 
# MAGIC     SUM(total_gmv) AS cumulative_gmv,
# MAGIC     SUM(completed_orders) AS total_completed_orders
# MAGIC FROM marketplace_gold.mart_daily_seller_performance
# MAGIC GROUP BY seller_name, category, city
# MAGIC ORDER BY cumulative_gmv DESC
# MAGIC LIMIT 5;
