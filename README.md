# 🏥 Pharma Sales ETL Pipeline

An end-to-end data engineering pipeline built using PySpark, Delta Lake, and AWS S3 following the Medallion Architecture pattern to process and analyze pharmaceutical sales data.

---

## 🏗️ Architecture

**pharma_sales.csv → AWS S3 → Bronze Layer → Silver Layer → Gold Layer**

| Layer | Tool | Description |
|---|---|---|
| Ingestion | AWS S3 + boto3 | Raw CSV stored and downloaded from S3 |
| Bronze | PySpark + Delta Lake | Raw ingestion, no transformations |
| Silver | PySpark + Delta Lake | Cleaned, deduplicated, schema enforced |
| Gold | PySpark + Delta Lake | 3 business-ready aggregated tables |

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

| File | Description |
|---|---|
| `bronze/bronze_layer.py` | Raw ingestion from S3 → Delta Lake |
| `silver/silver_layer.py` | Cleaning, deduplication, schema enforcement |
| `gold/gold_layer.py` | Business aggregations — 3 Gold tables |
| `data/pharma_sales.csv` | Sample pharma sales data |
| `config/config.py` | AWS credentials + paths (gitignored 🔐) |

---

## 📊 Gold Layer Outputs

| Table | Description |
|---|---|
| `sales_by_region` | Total and average sales per region |
| `sales_by_product` | Products ranked by revenue using dense_rank() |
| `monthly_trend` | Month-over-month revenue aggregated by year and month |

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
- Builds 3 aggregated business-ready tables
- Uses Window Functions + dense_rank() for product ranking
- Writes to separate Gold Delta paths

---

## ⚙️ How to Run

### Prerequisites
Install dependencies:
`pip install pyspark==3.5.0 boto3 delta-spark==3.2.0`

### Setup
Create `config/config.py` with your AWS credentials:

`AWS_ACCESS_KEY`, `AWS_SECRET_KEY`, `AWS_REGION`, `S3_BUCKET`, `S3_FILE_PATH`, `BRONZE_PATH`, `SILVER_PATH`, `GOLD_PATH`

Upload `pharma_sales.csv` to your S3 bucket.

### Run Pipeline
Run in order:

1. `py bronze/bronze_layer.py`
2. `py silver/silver_layer.py`
3. `py gold/gold_layer.py`

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
