# Real-Time Marketplace Intelligence Platform

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791.svg)](https://www.postgresql.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Distributed-black.svg)](https://kafka.apache.org/)
[![Debezium](https://img.shields.io/badge/CDC-Debezium-orange.svg)](https://debezium.io/)
[![Databricks](https://img.shields.io/badge/Compute-Databricks%20%7C%20PySpark-FF3621.svg)](https://community.cloud.databricks.com)
[![BigQuery](https://img.shields.io/badge/Warehouse-BigQuery%20%7C%20Delta-4285F4.svg)](https://cloud.google.com/bigquery)
[![Airflow](https://img.shields.io/badge/Orchestrator-Apache%20Airflow-017CEE.svg)](https://airflow.apache.org/)

An end-to-end, production-grade batch & streaming data engineering platform simulating a high-throughput online marketplace. Designed to tackle core data engineering challenges: CDC ingestion, event-time streaming with watermarks, star-schema modeling with SCD Type 2, PySpark skew optimization, BigQuery cost reduction, and automated data quality observability.

---

## Architecture Overview

```
                      +-----------------------------+
                      |    Python Data Generator    |
                      |   (Synthetic skew & late)   |
                      +--------------+--------------+
                                     |
               +---------------------+---------------------+
               |                                           |
               v                                           v
    +--------------------+                       +--------------------+
    | PostgreSQL (OLTP)  |                       |  Kafka Event Bus   |
    | Orders & Inventory |                       | (Views / Searches) |
    +----------+---------+                       +----------+---------+
               |                                            |
               v (WAL / CDC)                                |
    +--------------------+                                  |
    | Debezium Connector |                                  |
    +----------+---------+                                  |
               |                                            |
               v                                            v
    +-----------------------------------------------------------------+
    |                     Apache Kafka Topics                         |
    |  marketplace.orders | marketplace.payments | marketplace.views  |
    +--------------------------------+--------------------------------+
                                     |
                                     v
    +-----------------------------------------------------------------+
    |                 Databricks / PySpark Processing                 |
    |                                                                 |
    |   - Structured Streaming: Watermarking, deduplication, state    |
    |   - Batch Pipeline: Medallion transforms (Bronze -> Silver)     |
    |   - Skew Lab: Broadcast vs. Repartitioning vs. Key Salting      |
    +--------------------------------+--------------------------------+
                                     |
                                     v
    +-----------------------------------------------------------------+
    |              Analytical Storage & Data Warehouse                |
    |                                                                 |
    |   Bronze: Raw immutable events / Delta Lake                     |
    |   Silver: Cleaned, deduplicated, validated tables               |
    |   Gold:   Star schema (Facts, Dims with SCD Type 2)             |
    |           BigQuery / Delta Lake business aggregations           |
    +--------------------------------+--------------------------------+
                                     ^
                                     | Orchestrates & Observes
    +--------------------------------+--------------------------------+
    |             Apache Airflow + Data Quality Layer                 |
    |   Idempotent runs, backfills, freshness checks & schema SLAs    |
    +-----------------------------------------------------------------+
```

---

## Core Stack & Tooling

- **OLTP Source**: PostgreSQL 15 (Docker)
- **Change Data Capture (CDC)**: Debezium PostgreSQL Connector
- **Event Streaming**: Apache Kafka + Zookeeper / KRaft
- **Processing Engine**: **Databricks Community Edition** & PySpark (Structured Streaming + Batch)
- **Storage Layer**: Delta Lake & Google BigQuery / GCS
- **Orchestration**: Apache Airflow
- **Data Quality & Testing**: Custom PySpark assertion suite / Great Expectations style rule engine + Pytest

---

## Repository Structure

```
marketplace-data-platform/
├── README.md
├── docker-compose.yml
├── .env.example
├── .gitignore
├── data_generator/             # Scalable synthetic event & transactional generator
│   ├── config.yaml
│   ├── customers.py
│   ├── sellers.py
│   ├── products.py
│   ├── orders.py
│   ├── events.py
│   └── main.py
├── schemas/                    # Schemas for Kafka and Warehouse layers
│   ├── kafka/
│   └── warehouse/
├── spark/                      # Databricks & PySpark jobs
│   ├── batch/                  # Medallion ETL & SCD Type 2 processing
│   ├── streaming/              # Kafka Structured Streaming with watermarks
│   ├── jobs/                   # Production entrypoint jobs
│   └── notebooks/              # Databricks-compatible notebooks / Repos
├── airflow/                    # Workflow orchestration
│   ├── dags/
│   └── plugins/
├── quality/                    # Reusable quality test framework & assertions
│   ├── checks.py
│   └── rules.yaml
├── sql/                        # Warehouse schema definitions & analytics
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── monitoring/                 # Metrics, SLAs, and query cost tracking
│   └── metrics/
├── tests/                      # Unit & integration test suites
│   ├── unit/
│   └── integration/
└── docs/                       # Engineering deep-dives & experiment logs
    ├── architecture.md
    ├── data-model.md
    ├── skew-experiment.md
    ├── cost-optimization.md
    └── failure-scenarios.md
```

---

## Getting Started

### 1. Prerequisites
- Docker & Docker Compose
- Python 3.10+
- Free [Databricks Community Edition](https://community.cloud.databricks.com) account

### 2. Local Environment Setup
```bash
# Clone the repository
git clone https://github.com/ankit-024/marketplace-data-platform.git
cd marketplace-data-platform

# Create virtual environment
python -m venv venv
# Windows:
venv\Scripts\activate
# Linux/Mac:
# source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Syncing with Databricks Community Edition
1. In your Databricks Community Edition workspace, open **Workspace** > **Users** > your user folder.
2. Click **Create** > **Git Folder**.
3. Enter your GitHub repo URL: `https://github.com/ankit-024/marketplace-data-platform.git`.
4. Databricks will sync the `spark/` modules and notebooks directly into your cluster!

---

## Milestones & Development Roadmap

- [ ] **Milestone 1**: Synthetic Data Generator + PostgreSQL schema setup + Star Schema design
- [ ] **Milestone 2**: PySpark Batch Pipeline on Databricks (Bronze -> Silver -> Gold SCD Type 2)
- [ ] **Milestone 3**: Debezium CDC + Kafka Event Streaming
- [ ] **Milestone 4**: Databricks Structured Streaming with watermarking & deduplication
- [ ] **Milestone 5**: Data Skew Experiment Lab (Salting vs Broadcast vs AQE)
- [ ] **Milestone 6**: Airflow Orchestration & Data Quality Framework
- [ ] **Milestone 7**: BigQuery / Delta Warehouse Cost Optimization & Benchmark Report
