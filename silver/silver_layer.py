import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += ";C:\\hadoop\\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, to_date, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from delta import configure_spark_with_delta_pip
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import BRONZE_PATH, SILVER_PATH

# ── Create Spark Session ─────────────────────────────────────────────────────
builder = SparkSession.builder \
    .appName("Pharma Pipeline - Silver Layer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("✅ Spark Session created!")

# ── Read from Bronze Delta Layer ─────────────────────────────────────────────
print("⏳ Reading from Bronze layer...")

bronze_df = spark.read.format("delta").load(BRONZE_PATH)

print(f"✅ Bronze data loaded! Records: {bronze_df.count()}")

# ── Define Explicit Schema ────────────────────────────────────────────────────
print("⏳ Applying explicit schema...")

silver_df = bronze_df.select(
    col("order_id").cast(IntegerType()).alias("order_id"),
    trim(col("product")).alias("product"),
    trim(upper(col("region"))).alias("region"),
    trim(upper(col("category"))).alias("category"),
    col("amount").cast(IntegerType()).alias("amount"),
    col("order_date").cast(DateType()).alias("order_date")
)

print("✅ Schema applied!")

# ── Deduplication ─────────────────────────────────────────────────────────────
print("⏳ Removing duplicates...")

before_count = silver_df.count()

silver_df = silver_df.dropDuplicates(["order_id"])

after_count = silver_df.count()

print(f"✅ Deduplication done! Before: {before_count} → After: {after_count} records")

# ── Add Audit Column ──────────────────────────────────────────────────────────
print("⏳ Adding audit columns...")

silver_df = silver_df.withColumn("ingested_at", current_timestamp())

print("✅ Audit column added!")
silver_df.show(5, truncate=False)
silver_df.printSchema()

# ── Write to Silver Delta Layer ───────────────────────────────────────────────
print("⏳ Writing to Silver Delta layer...")

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(SILVER_PATH)

print(f"✅ Silver layer written to {SILVER_PATH}!")
print("🎉 Silver Layer Complete!")

spark.stop()