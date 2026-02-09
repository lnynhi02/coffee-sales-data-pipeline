# ===========================================================================
# SCRIPT: Incremental Load for Orders Table and Order Details to Silver Layer
# ===========================================================================
# Purpose:
#   - Reads raw order data from the Bronze Layer.
#   - Performs an incremental load based on the latest updated timestamp.
#   - Assumes the data is clean and requires minimal transformation.
#   - Writes data to the Silver Layer in Parquet format for analytical use.
# ===========================================================================
import sys
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(BASE_DIR)

from pathlib import Path
from loguru import logger

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from src.utils.spark_helper import *

# Setup logging
LOG_FILE = BASE_DIR / "logs" / "batch.log"
logger.remove()

logger.add(
    LOG_FILE, 
    rotation="00:00",
    encoding="utf-8",
    enqueue=True
)


def read_bronze_layer(spark: SparkSession, table):
    return spark.read.parquet(f"s3a://bronze-layer/{table}")


def detect_new_orders(spark: SparkSession):
    bronze_order = read_bronze_layer(spark, table="brz.orders")

    if check_minio_has_data(bucket="silver-layer", prefix="slv.orders/"):
        logger.info("[SILVER][slv.orders] Existing data detected ->  Checking for new records...")
        existing_df = spark.read.parquet("s3a://silver-layer/slv.orders")
        latest_time = existing_df.select(max("timestamp")).first()[0]
        new_orders = bronze_order.filter(col("timestamp") > latest_time)
    else:
        logger.info("[SILVER][slv.orders] No Silver data found -> Performing initial full load from Bronze Layer.")
        new_orders = bronze_order

    return new_orders


def write_orders_and_details(spark, silver_path, new_orders):
    """
    The 'orders' and 'order_details' tables have already been cleaned, 
    so we simply load them into the Silver layer.
    """
    bronze_order_details = read_bronze_layer(spark, table="brz.order_details")
    bronze_order_suggestion = read_bronze_layer(spark, table="brz.order_suggestion_accepted")

    if new_orders.isEmpty():
        logger.debug("[SILVER][slv.orders] No new orders found -> Skipping write operation.")
        logger.debug("[SILVER][slv.order_details] No new orders found -> Skipping write operation.")  

    else:
        logger.info(f"[SILVER][slv.orders] Found new records. Preparing to process and write...")

        new_order_details = new_orders.join(
            bronze_order_details,
            new_orders["id"] == bronze_order_details["order_id"],
            how="inner"
        ).select(
            "order_id", "product_id", "quantity", "discount_percent", "subtotal", "is_suggestion",
            bronze_order_details.year, bronze_order_details.month, bronze_order_details.day
        )

        # Add suggested products to order details
        new_order_details = new_order_details.union(bronze_order_suggestion)

        new_orders.withColumnRenamed("id", "order_id") \
            .write.partitionBy("year", "month", "day") \
            .mode("append") \
            .parquet(f"{silver_path}/slv.orders")

        new_order_details.write.partitionBy("year", "month", "day") \
            .mode("append") \
            .parquet(f"{silver_path}/slv.order_details")

        logger.success("[SILVER][slv.orders] Successfully appended new records.")
        logger.success("[SILVER][slv.order_details] Successfully appended new records.")


def main():
    logger.info("======== [SILVER] START: Loading 'orders' data from Bronze Layer to Silver Layer ========")
    try:
        spark = create_SparkSession(app_name="brz.orders+order_details→slv.orders+order_details")

        new_orders = detect_new_orders(spark)
        write_orders_and_details(spark, silver_path="s3a://silver-layer", new_orders=new_orders)

        spark.stop()
    except Exception as e:
        logger.error(f"Error during transformation: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()