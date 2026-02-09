# =======================================================================
# Purpose:
#   - Validate and inspect the final schema and data in the gold layer
# =======================================================================

import logging
from pathlib import Path

from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parent.parent

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
)

def create_SparkSession() -> SparkSession:
    return SparkSession.builder \
        .appName("Load Silver to Gold Layer") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def read_gold_layer(spark, table):
    try:
        df = spark.read.format("delta").load(f"s3a://gold-layer/{table}")
        logging.info(f"Table: {table} | Count: {df.count()}")
        df.printSchema()
        df.show(5, truncate=False)
    except Exception as e:
        logging.error(f"Failed to read table '{table}': {e}")


if __name__ == "__main__":
    spark = create_SparkSession()
    tables = ["gld.dim_stores", "gld.dim_products", "gld.dim_payment", "gld.fact_orders"]

    for table in tables:
        logging.info(f"--- Reading table: {table} ---")
        read_gold_layer(spark, table)

    spark.stop()