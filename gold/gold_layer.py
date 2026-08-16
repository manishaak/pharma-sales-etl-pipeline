import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += ";C:\\hadoop\\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, avg, dense_rank, month, year, round, col
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import SILVER_PATH, GOLD_PATH

# ── Create Spark Session ─────────────────────────────────────────────────────
builder = SparkSession.builder \
    .appName("Pharma Pipeline - Gold Layer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("✅ Spark Session created!")

# ── Read from Silver ─────────────────────────────────────────────────────────
print("⏳ Reading from Silver layer...")
silver_df = spark.read.format("delta").load(SILVER_PATH)
print(f"✅ Silver data loaded! Records: {silver_df.count()}")

# ── Gold Table 1: Sales by Region ────────────────────────────────────────────
print("⏳ Building Gold Table 1 — Sales by Region...")

sales_by_region = silver_df \
    .groupBy("region") \
    .agg(
        spark_sum("amount").alias("total_sales"),
        round(avg("amount"), 2).alias("avg_sales"),
        ) \
    .orderBy("total_sales", ascending=False)

sales_by_region.show()

sales_by_region.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{GOLD_PATH}/sales_by_region")

print("✅ Gold Table 1 written!")

# ── Gold Table 2: Sales by Product (Ranked) ──────────────────────────────────
print("⏳ Building Gold Table 2 — Sales by Product...")

window_spec = Window.orderBy(col("total_sales").desc())

sales_by_product = silver_df \
    .groupBy("product") \
    .agg(spark_sum("amount").alias("total_sales")) \
    .withColumn("rank", dense_rank().over(window_spec))

sales_by_product.show()

sales_by_product.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{GOLD_PATH}/sales_by_product")

print("✅ Gold Table 2 written!")

# ── Gold Table 3: Monthly Sales Trend ────────────────────────────────────────
print("⏳ Building Gold Table 3 — Monthly Sales Trend...")

monthly_trend = silver_df \
    .withColumn("year", year("order_date")) \
    .withColumn("month", month("order_date")) \
    .groupBy("year", "month") \
    .agg(spark_sum("amount").alias("monthly_revenue")) \
    .orderBy("year", "month")

monthly_trend.show()

monthly_trend.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{GOLD_PATH}/monthly_trend")

print("✅ Gold Table 3 written!")
print("🎉 Gold Layer Complete!")

spark.stop()