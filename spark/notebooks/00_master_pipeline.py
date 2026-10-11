# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Master End-to-End Medallion Pipeline
# MAGIC 
# MAGIC **Marketplace Intelligence Platform**  
# MAGIC Orchestrates the complete pipeline in sequence:
# MAGIC 1. **Bronze Ingestion**: Syncs latest orders from Neon PostgreSQL to Delta Lake
# MAGIC 2. **Silver Cleaning**: Deduplication, casting, and date partitioning
# MAGIC 3. **Gold Star Schema**: SCD Type 2 updates, Fact tables, and Daily Business Marts

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1. Run Bronze Layer

# COMMAND ----------
print(">>> [1/3] Starting Bronze Ingestion from Neon PostgreSQL...")
bronze_result = dbutils.notebook.run("01_bronze_ingestion", timeout_seconds=600)
print(">>> Bronze Ingestion Complete!")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2. Run Silver Layer

# COMMAND ----------
print(">>> [2/3] Starting Silver Cleaning & Deduplication...")
silver_result = dbutils.notebook.run("02_silver_cleaning", timeout_seconds=600)
print(">>> Silver Cleaning Complete!")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3. Run Gold Layer (Star Schema & SCD Type 2)

# COMMAND ----------
print(">>> [3/3] Starting Gold Star Schema & Business Marts...")
gold_result = dbutils.notebook.run("03_gold_star_schema_scd2", timeout_seconds=600)
print(">>> Gold Layer Complete!")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4. Pipeline Execution Summary

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     'Bronze Orders' AS layer, COUNT(*) AS total_rows FROM marketplace_bronze.orders
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Silver Clean Orders' AS layer, COUNT(*) AS total_rows FROM marketplace_silver.orders_clean
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Gold Fact Orders' AS layer, COUNT(*) AS total_rows FROM marketplace_gold.fact_orders;
