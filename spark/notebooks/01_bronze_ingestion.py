# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Bronze Layer: Raw Ingestion from Neon Cloud PostgreSQL
# MAGIC 
# MAGIC **Marketplace Intelligence Platform**  
# MAGIC Ingests transactional data from operational PostgreSQL tables via PySpark JDBC and writes raw immutable **Delta Lake** tables.
# MAGIC 
# MAGIC ### Core Concepts:
# MAGIC - **JDBC Extraction**: Read with partition bounds for scalability.
# MAGIC - **Bronze Principles**: Keep data raw and immutable; append system metadata (`_ingested_at`, `_source_system`).

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1. Connection Configuration
# MAGIC Neon Cloud PostgreSQL credentials (can be overridden via Databricks Widgets or Secrets).

# COMMAND ----------
dbutils.widgets.text("neon_host", "ep-gentle-pine-b4cj4y39-pooler.c-6.us-east-2.aws.neon.tech")
dbutils.widgets.text("neon_port", "5432")
dbutils.widgets.text("neon_db", "neondb")
dbutils.widgets.text("neon_user", "neondb_owner")
dbutils.widgets.text("neon_password", "npg_Bs1TRKDhfei2")

neon_host = dbutils.widgets.get("neon_host")
neon_port = dbutils.widgets.get("neon_port")
neon_db = dbutils.widgets.get("neon_db")
neon_user = dbutils.widgets.get("neon_user")
neon_password = dbutils.widgets.get("neon_password")

jdbc_url = f"jdbc:postgresql://{neon_host}:{neon_port}/{neon_db}?ssl=true&sslmode=require"
connection_properties = {
    "user": neon_user,
    "password": neon_password,
    "driver": "org.postgresql.Driver"
}

print(f"JDBC Target URL: jdbc:postgresql://{neon_host}:{neon_port}/{neon_db}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2. Initialize Bronze Delta Database
# MAGIC Create the `marketplace_bronze` database in Databricks metastore.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS marketplace_bronze;

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3. Ingest PostgreSQL Tables into Bronze Delta Tables
# MAGIC We read: `customers`, `sellers`, `products`, `orders`, `payments`, `inventory_events`.

# COMMAND ----------
from pyspark.sql.functions import current_timestamp, lit

tables_to_ingest = [
    "customers",
    "sellers",
    "products",
    "orders",
    "payments",
    "inventory_events"
]

for table_name in tables_to_ingest:
    print(f"Ingesting '{table_name}' from Neon PostgreSQL...")
    
    # Read table from PostgreSQL
    df_raw = spark.read.jdbc(
        url=jdbc_url,
        table=table_name,
        properties=connection_properties
    )
    
    # Append Bronze metadata
    df_bronze = df_raw \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source_system", lit("neon_postgres_oltp"))
    
    # Save as Delta Table (Overwrites for batch snapshot / append for CDC)
    target_table = f"marketplace_bronze.{table_name}"
    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    
    count = spark.table(target_table).count()
    print(f"  -> Saved '{target_table}' ({count:,} records)")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4. Bronze Ingestion Summary & Sanity Verification

# COMMAND ----------
# MAGIC %sql
# MAGIC SHOW TABLES IN marketplace_bronze;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT * FROM marketplace_bronze.orders LIMIT 5;
