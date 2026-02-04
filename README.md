# develop

## Overview
This project simulates a **real-world banking transaction data engineering pipeline** built using **PySpark and Python**.  
It demonstrates how large-scale transactional data can be ingested, validated, transformed, and aggregated efficiently using **Spark best practices**.

The design mirrors enterprise batch-processing use cases typically seen in **banking and financial institutions**, focusing on:
- Data quality
- Scalability
- Performance optimization
- Clean modular architecture

---

## Business Problem
Banks process **millions of transactions daily** from multiple channels (cards, transfers, payments).  
Raw transactional data is often:
- Incomplete
- Duplicated
- Delayed
- Prone to schema inconsistencies

The objective of this pipeline is to:
- Ingest raw banking data
- Apply data quality rules
- Create analytics-ready datasets
- Enable daily reporting and customer insights


## Architecture (Medallion Pattern)

### Layer Responsibilities
- **Bronze**: Schema enforcement and raw storage  
- **Silver**: Data quality checks, cleansing, enrichment  
- **Gold**: Business-level aggregations for reporting  

---

## Tech Stack
- **PySpark**
- **Python 3**
- **Spark SQL**
- **Jupyter Notebooks**
- **YAML (Spark Configs)**


---

## Data Pipeline Flow

### Data Ingestion (Bronze)
- Explicit schema enforcement (no inferSchema)
- Reads raw CSV files using PySpark
- Prevents corrupt or malformed records from entering downstream layers

### Data Quality Checks (Silver)
- Null validation
- Duplicate detection
- Invalid transaction filtering (negative amounts, missing timestamps)
- Invalid records quarantined instead of dropped

### Transformations & Enrichment
- Join transactions with customer data
- Derived columns:
  - Transaction date
  - High-value transaction flag
- Window-based transformations for analytics readiness

### 4Aggregations (Gold)
- Daily customer spend
- Transaction counts per customer
- Date-partitioned analytical tables


## Performance Optimizations
The pipeline applies multiple Spark optimization techniques:
- Partitioning by transaction date
- Broadcast joins for small dimension tables
- Repartition vs coalesce usage
- Caching frequently accessed datasets
- Spark execution plan analysis using `explain()`

---

## Sample Analytics Outputs
- Daily customer transaction volume
- High-value transaction trends
- Customer spending behavior

---

## How to Run Locally

### Prerequisites
- Python 3.8+
- Apache Spark
- Java 8 or 11

### Install Dependencies
```bash
pip install -r requirements.txt
