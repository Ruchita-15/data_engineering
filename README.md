# Banking Transaction Data Engineering Pipeline

## Overview
A **production-grade data engineering pipeline** that processes banking transactions using the **Medallion Architecture**. Built with **PySpark**, this project demonstrates real-world patterns for ingesting, validating, transforming, and aggregating large-scale transactional data.

**Key Features:**
- Multi-layer data pipeline (Bronze → Silver → Gold)
- Comprehensive data quality checks and validation
- Advanced Spark optimizations (partitioning, broadcasting, caching)
- Scalable modular architecture
- Interactive Jupyter notebooks for exploration and analysis

---

## Business Context
Banks process **millions of daily transactions** from multiple channels (cards, transfers, online payments). Raw data typically has:
- Missing or incomplete records
- Duplicate transactions
- Delayed arrivals
- Schema inconsistencies

**Solution:** This pipeline automates ingestion, quality assurance, and analytics-ready aggregations, enabling daily reporting and customer insights.

---

## Architecture: Medallion Pattern

```
Raw Data (CSV)
    ↓
┌─────────────────┐
│  BRONZE LAYER   │  → Schema enforcement, raw storage
│  (Ingestion)    │
└─────────────────┘
    ↓
┌─────────────────┐
│  SILVER LAYER   │  → Validation, cleansing, enrichment
│  (Transformation)│
└─────────────────┘
    ↓
┌─────────────────┐
│  GOLD LAYER     │  → Analytics, reporting, aggregations
│  (Aggregation)  │
└─────────────────┘
```

### Layer Details

| Layer | Purpose | Operations |
|-------|---------|-----------|
| **Bronze** | Raw data storage with schema enforcement | CSV ingestion, schema validation, error quarantine |
| **Silver** | Clean, enriched data | Null checks, duplicate detection, invalid record filtering, customer joins, feature engineering |
| **Gold** | Analytics-ready aggregations | Daily spend by customer, transaction counts, date-partitioned reporting tables |

---

## Tech Stack
- **PySpark 3.4.1** — Distributed data processing
- **Python 3.8+** — Core programming language
- **Spark SQL** — Structured data queries
- **Jupyter Notebooks** — Interactive exploration and documentation
- **YAML** — Configuration management
- **Pandas & NumPy** — Data utilities

---

## Project Structure

```
data_engineering/
├── README.md                          # This file
├── requirements.txt                   # Python dependencies
├── dummy_data_generator.py            # Generate sample banking data
├── configs/
│   └── spark_config.yaml              # Spark cluster configuration
├── data/
│   ├── raw/                           # Source CSV files
│   │   ├── customers.csv
│   │   ├── accounts.csv
│   │   └── transactions.csv
│   └── processed/
│       ├── bronze/                    # Raw ingested data
│       ├── silver/                    # Cleaned & enriched data
│       └── gold/                      # Aggregated analytics
├── notebooks/                         # Interactive exploration
│   ├── 01_ingestion.ipynb            # Bronze layer
│   ├── 02_data_quality.ipynb         # Quality checks
│   ├── 03_transformations.ipynb      # Silver layer
│   ├── 04_aggregations.ipynb         # Gold layer
│   └── 05_performance_optimizations.ipynb
└── src/                              # Reusable modules
    ├── ingestion.py                  # Data ingestion logic
    ├── validations.py                # Quality rules
    ├── transformations.py            # Data transformation
    ├── aggregations.py               # Aggregation logic
    ├── utils.py                      # Common utilities
    └── __init__.py
```

---

## Data Flow & Processing

### 1. Data Ingestion (Bronze Layer)
**Goal:** Ingest raw data with strict schema enforcement

```python
# Explicit schema (no inferSchema) prevents schema drift
df = spark.read \
    .schema(transactions_schema) \
    .csv("data/raw/transactions.csv", header=True)

df.write.parquet("data/processed/bronze/transactions/")
```

**Features:**
- Strict schema validation
- Early error detection
- Bad records quarantined (not dropped)

### 2. Data Quality Checks (Silver Layer)
**Goal:** Validate, cleanse, and enrich data

**Validation Rules:**
- ✓ No null transaction IDs
- ✓ No duplicate transactions
- ✓ Transaction amount > 0
- ✓ Valid timestamps
- ✓ Customer exists in master data

**Transformations:**
- Flagging high-value transactions
- Extracting transaction date
- Joining with customer/account metadata
- Quarantining invalid records separately

### 3. Transformations & Enrichment (Silver Layer)
**Goal:** Create feature-rich datasets for analytics

```python
# Join transactions with customer data
enriched = transactions \
    .join(customers, "customer_id", "left") \
    .withColumn("high_value_flag", 
                col("amount") > 10000) \
    .withColumn("txn_date", to_date(col("timestamp")))
```

### 4. Aggregations (Gold Layer)
**Goal:** Pre-computed analytics for reporting

**Current Outputs:**
- **Daily Customer Spend** — Total spend per customer per date (partitioned by `txn_date`)
- **Customer Spend Summary** — Monthly/weekly aggregations
- **Transaction Counts** — Volume metrics by customer/channel

**Partitioning Strategy:**
```
gold/daily_customer_spend/
├── txn_date=2025-02-05/part-0001.parquet
├── txn_date=2025-02-06/part-0001.parquet
├── ...
```

---

## Performance Optimizations

This pipeline implements enterprise-grade Spark optimizations:

| Technique | Benefit | Usage |
|-----------|---------|-------|
| **Partitioning by date** | Faster queries, easy incremental loads | Gold layer tables |
| **Broadcast joins** | Reduces shuffle for small dims | Customer/Account joins |
| **Caching** | Faster downstream operations | Frequently accessed DFs |
| **Coalesce vs Repartition** | Optimized file writing | Final write operations |
| **Explain plans** | Identify bottlenecks | Development & debugging |

**Example:**
```python
# Broadcast small dimension table to all nodes
enriched = transactions \
    .join(broadcast(customers), "customer_id", "left")

# Cache for reuse
transactions.cache()
```

---

## Installation & Setup

### Prerequisites
- **Python 3.8+**
- **Java 8 or 11** (required for Spark)
- **Apache Spark 3.4.1** (bundled via pip)
- **4GB RAM minimum** (for local testing)

### 1. Clone & Install
```bash
cd /path/to/data_engineering
pip install -r requirements.txt
```

### 2. Generate Sample Data
```bash
python dummy_data_generator.py
```
Creates:
- `data/raw/customers.csv` (1000 customers)
- `data/raw/accounts.csv` (1500 accounts)
- `data/raw/transactions.csv` (20000 transactions)

### 3. Run the Pipeline

**Option A: Interactive Notebooks**
```bash
jupyter notebook notebooks/
```
Then open and run:
1. `01_ingestion.ipynb` — Ingest raw data
2. `02_data_quality.ipynb` — Validate data
3. `03_transformations.ipynb` — Transform & enrich
4. `04_aggregations.ipynb` — Create analytics
5. `05_performance_optimizations.ipynb` — Benchmark & optimize

**Option B: Programmatic (Python)** (Coming soon in `src/main.py`)

---

## Sample Output

After running the pipeline, you'll have:

```
data/processed/
├── bronze/transactions/          # Raw ingested parquet
├── silver/
│   ├── transactions_clean/       # Validated transactions
│   ├── transactions_enriched/    # With customer/account data
│   └── transactions_invalid/     # Quarantined bad records
└── gold/
    └── daily_customer_spend/     # Pre-aggregated for BI
```

**Example Query (Gold Layer):**
```python
daily_spend = spark.read.parquet("data/processed/gold/daily_customer_spend/")
daily_spend.filter(col("txn_date") == "2025-02-05").show()

# Output:
# +-------------+----------+----------+
# |customer_id  |txn_date  |total_spend|
# +-------------+----------+----------+
# |CUST00001    |2025-02-05|  45320.50 |
# |CUST00002    |2025-02-05|  12100.00 |
# ...
```

---

## Development Guide

### Adding New Transformations
1. Add logic to `src/transformations.py`
2. Import in `notebooks/03_transformations.ipynb`
3. Test and validate output

### Adding Quality Rules
1. Update `src/validations.py`
2. Apply in `notebooks/02_data_quality.ipynb`
3. Monitor invalid record counts

### Configuration
Edit `configs/spark_config.yaml` to adjust:
- Executor memory
- Number of partitions
- Shuffle partition count

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `Java not found` | Install Java 8/11; set `JAVA_HOME` |
| `Out of memory` | Reduce `NUM_TRANSACTIONS` in `dummy_data_generator.py` or increase system RAM |
| `Parquet file not found` | Run `dummy_data_generator.py` first to create sample data |
| Slow performance | Run `05_performance_optimizations.ipynb` to analyze execution plans |

---

## Next Steps / Roadmap
- [ ] Incremental data loading (CDC)
- [ ] Real-time streaming pipeline (Kafka + Structured Streaming)
- [ ] Data lineage tracking
- [ ] Automated testing & CI/CD
- [ ] Production deployment on Databricks/EMR

---
