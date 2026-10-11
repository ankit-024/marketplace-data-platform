# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Silver Layer: Cleaning, Validation & Deduplication
# MAGIC 
# MAGIC **Marketplace Intelligence Platform**  
# MAGIC Transforms raw **Bronze** data into curated, cleaned, and validated **Silver Delta tables**.
# MAGIC 
# MAGIC ### Core Engineering Principles:
# MAGIC - **Deduplication**: Handle duplicate events from at-least-once delivery.
# MAGIC - **Type Safety**: Enforce strict schemas, decimal precisions, and timestamps.
# MAGIC - **Quality Filtering**: Discard invalid records (e.g. negative quantities, orphan records).
# MAGIC - **Partitioning**: Partition large transactional tables by `order_date` for query pruning.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS marketplace_silver;

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1. Clean Orders (`marketplace_silver.orders_clean`)

# COMMAND ----------
from pyspark.sql.functions import col, to_date, trim, lower, upper, to_timestamp

df_bronze_orders = spark.table("marketplace_bronze.orders")

df_silver_orders = df_bronze_orders \
    .filter(col("order_id").isNotNull() & (col("quantity") > 0) & (col("order_amount") >= 0)) \
    .dropDuplicates(["order_id"]) \
    .withColumn("order_ts", to_timestamp(col("order_ts"))) \
    .withColumn("ingested_ts", to_timestamp(col("ingested_ts"))) \
    .withColumn("order_date", to_date(col("order_ts"))) \
    .withColumn("order_amount", col("order_amount").cast("decimal(12,2)")) \
    .withColumn("unit_price", col("unit_price").cast("decimal(10,2)")) \
    .withColumn("quantity", col("quantity").cast("int")) \
    .withColumn("status", upper(trim(col("status"))))

df_silver_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("order_date") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marketplace_silver.orders_clean")

print(f"Saved 'marketplace_silver.orders_clean': {spark.table('marketplace_silver.orders_clean').count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2. Clean Payments (`marketplace_silver.payments_clean`)

# COMMAND ----------
df_bronze_payments = spark.table("marketplace_bronze.payments")

df_silver_payments = df_bronze_payments \
    .filter(col("payment_id").isNotNull()) \
    .dropDuplicates(["payment_id"]) \
    .withColumn("payment_ts", to_timestamp(col("payment_ts"))) \
    .withColumn("payment_amount", col("payment_amount").cast("decimal(12,2)")) \
    .withColumn("payment_method", upper(trim(col("payment_method")))) \
    .withColumn("payment_status", upper(trim(col("payment_status"))))

df_silver_payments.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marketplace_silver.payments_clean")

print(f"Saved 'marketplace_silver.payments_clean': {spark.table('marketplace_silver.payments_clean').count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3. Clean Sellers (`marketplace_silver.sellers_clean`)

# COMMAND ----------
df_bronze_sellers = spark.table("marketplace_bronze.sellers")

df_silver_sellers = df_bronze_sellers \
    .filter(col("seller_id").isNotNull()) \
    .dropDuplicates(["seller_id"]) \
    .withColumn("seller_name", trim(col("seller_name"))) \
    .withColumn("category", trim(col("category"))) \
    .withColumn("city", trim(col("city"))) \
    .withColumn("state", trim(col("state"))) \
    .withColumn("rating", col("rating").cast("decimal(3,2)")) \
    .withColumn("registration_ts", to_timestamp(col("registration_ts")))

df_silver_sellers.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marketplace_silver.sellers_clean")

print(f"Saved 'marketplace_silver.sellers_clean': {spark.table('marketplace_silver.sellers_clean').count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4. Clean Products (`marketplace_silver.products_clean`)

# COMMAND ----------
df_bronze_products = spark.table("marketplace_bronze.products")

df_silver_products = df_bronze_products \
    .filter(col("product_id").isNotNull() & (col("price") > 0)) \
    .dropDuplicates(["product_id"]) \
    .withColumn("product_name", trim(col("product_name"))) \
    .withColumn("category", trim(col("category"))) \
    .withColumn("sub_category", trim(col("sub_category"))) \
    .withColumn("price", col("price").cast("decimal(10,2)")) \
    .withColumn("cost", col("cost").cast("decimal(10,2)")) \
    .withColumn("stock_quantity", col("stock_quantity").cast("int")) \
    .withColumn("created_ts", to_timestamp(col("created_ts")))

df_silver_products.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marketplace_silver.products_clean")

print(f"Saved 'marketplace_silver.products_clean': {spark.table('marketplace_silver.products_clean').count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5. Clean Customers (`marketplace_silver.customers_clean`)

# COMMAND ----------
df_bronze_customers = spark.table("marketplace_bronze.customers")

df_silver_customers = df_bronze_customers \
    .filter(col("customer_id").isNotNull()) \
    .dropDuplicates(["customer_id"]) \
    .withColumn("full_name", trim(col("full_name"))) \
    .withColumn("email", lower(trim(col("email")))) \
    .withColumn("city", trim(col("city"))) \
    .withColumn("state", trim(col("state"))) \
    .withColumn("segment", upper(trim(col("segment")))) \
    .withColumn("signup_ts", to_timestamp(col("signup_ts")))

df_silver_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marketplace_silver.customers_clean")

print(f"Saved 'marketplace_silver.customers_clean': {spark.table('marketplace_silver.customers_clean').count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 6. Silver Validation Summary

# COMMAND ----------
# MAGIC %sql
# MAGIC SHOW TABLES IN marketplace_silver;
