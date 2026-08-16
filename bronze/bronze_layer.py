import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += ";C:\\hadoop\\bin"



import boto3
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import sys
import os

# Add parent directory to path for config import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import *

# ── 1. Create Spark Session with Delta Lake ──────────────────────────────────
builder = SparkSession.builder \
    .appName("Pharma Pipeline - Bronze Layer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("✅ Spark Session created successfully!")

# ── 2. Download CSV from S3 using boto3 ─────────────────────────────────────
print("⏳ Downloading pharma_sales.csv from S3...")

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

s3_client.download_file(S3_BUCKET, S3_FILE_PATH, "data/pharma_sales.csv")
print("✅ File downloaded from S3 successfully!")

# ── 3. Read Raw CSV into PySpark DataFrame ───────────────────────────────────
print("⏳ Reading raw CSV into PySpark...")

bronze_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/pharma_sales.csv")

print("✅ Raw data loaded!")
print(f"   Total records: {bronze_df.count()}")
print(f"   Columns: {bronze_df.columns}")

# ── 4. Show sample data ──────────────────────────────────────────────────────
print("\n📊 Sample Bronze Data:")
bronze_df.show(5, truncate=False)
bronze_df.printSchema()

# ── 5. Write to Delta Lake (Bronze Layer) ────────────────────────────────────
print("⏳ Writing to Delta Lake Bronze layer...")

bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(BRONZE_PATH)

print(f"✅ Bronze layer written successfully to {BRONZE_PATH}!")
print("🎉 Bronze Layer Complete!")

spark.stop()