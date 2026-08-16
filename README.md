# 🏥 Pharma Sales ETL Pipeline

An end-to-end data engineering pipeline built using PySpark, Delta Lake, AWS S3,
and Medallion Architecture to process and analyze pharmaceutical sales data.

---

## 🏗️ Architecture
pharma_sales.csv
↓
AWS S3 (Raw Storage)
↓ boto3
Bronze Layer (Delta Lake)
→ Raw ingestion, no transformations
↓ PySpark
Silver Layer (Delta Lake)
→ Cleaned, deduplicated, schema enforced, audit columns
↓ PySpark
Gold Layer (Delta Lake)
→ 3 business-ready aggregated tables
---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| PySpark 3.5 | Distributed data processing |
| Delta Lake 3.2 | ACID transactions, versioning |
| AWS S3 | Cloud raw data storage |
| boto3 | AWS SDK for S3 connection |
| Python 3.14 | Pipeline scripting |

---

## 📁 Project Structure

pharma-pipeline/
│
├── bronze/
│ └── bronze_layer.py # Raw ingestion from S3 → Delta Lake
│
├── silver/
│ └── silver_layer.py # Cleaning, deduplication, schema enforcement
│
├── gold/
│ └── gold_layer.py # Business aggregations
│
├── data/
│ └── pharma_sales.csv # Sample pharma sales data
│
├── config/
│ └── config.py # AWS credentials + paths (gitignored)
│
└── README.md

---

## 📊 Gold Layer Outputs

### 1. Sales by Region
Total and average pharma sales per region, ordered by revenue.

### 2. Sales by Product (Ranked)
All products ranked by total revenue using Window Functions + dense_rank().

### 3. Monthly Sales Trend
Month-over-month revenue aggregated by year and month.

---

## 🔄 Pipeline Layers

### Bronze Layer
- Downloads raw CSV from AWS S3 using boto3
- Reads into PySpark DataFrame with schema inference
- Writes to Delta Lake as-is (no transformations)
- Idempotent — safe to re-run (overwrite mode)

### Silver Layer
- Reads from Bronze Delta table
- Enforces explicit schema (no inferSchema)
- Standardizes region and category to uppercase
- Removes duplicates based on order_id (primary key)
- Adds ingested_at audit timestamp
- Writes cleaned data to Silver Delta table

### Gold Layer
- Reads from Silver Delta table
- Builds 3 aggregated business tables:
  - sales_by_region
  - sales_by_product (with dense_rank)
  - monthly_trend
- Writes to separate Gold Delta paths

---

## ⚙️ How to Run

### Prerequisites
```bash
pip install pyspark==3.5.0 boto3 delta-spark==3.2.0
```

### Setup
1. Create `config/config.py` with your AWS credentials:
```python
AWS_ACCESS_KEY = "your_access_key"
AWS_SECRET_KEY = "your_secret_key"
AWS_REGION = "ap-south-1"
S3_BUCKET = "your-bucket-name"
S3_FILE_PATH = "pharma_sales.csv"
BRONZE_PATH = "delta/bronze/pharma_sales"
SILVER_PATH = "delta/silver/pharma_sales"
GOLD_PATH = "delta/gold/pharma_sales"
```

2. Upload `pharma_sales.csv` to your S3 bucket

### Run Pipeline
```bash
# Step 1 — Bronze Layer
py bronze/bronze_layer.py

# Step 2 — Silver Layer
py silver/silver_layer.py

# Step 3 — Gold Layer
py gold/gold_layer.py
```

---

## 💡 Key Concepts Demonstrated

- **Medallion Architecture** — Bronze → Silver → Gold
- **Delta Lake** — ACID transactions, time travel, schema enforcement
- **Idempotent Pipelines** — Safe to re-run without duplicates
- **Window Functions** — dense_rank() for product rankings
- **Data Quality** — Deduplication, standardization, audit columns
- **AWS S3 Integration** — boto3 for cloud storage connectivity

---

## 👩‍💻 Author
Manisha Kumari | [LinkedIn](https://www.linkedin.com/in/manisha-kumari-52686415b/)

---

## 📁 Project Structure
