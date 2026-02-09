# ==================================================================================
# SCRIPT: Create Fact Table 'gld.fact_orders'
# ==================================================================================
# Purpose:
#   - Create the fact table 'gld.fact_orders' in the Gold Layer using Delta format
#   - Store detailed transactional data at the order line level
#   - Include surrogate keys referencing dimension tables (e.g., product_key, store_key, payment_method_key)
#   - Store numeric measures like quantity and subtotal for accurate revenue calculations
#
# Usage:
#   - Used as the main fact table for sales analytics, revenue reporting, and business KPIs
# ==================================================================================
import sys
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(BASE_DIR)

from pathlib import Path
from loguru import logger

from pyspark.sql.functions import *

from src.utils.spark_helper import *


# Setup logging
LOG_FILE = Path("logs/batch.log")
logger.remove()

logger.add(
    LOG_FILE, 
    rotation="00:00",
    encoding="utf-8",
    enqueue=True
)


def read_silver_layer(spark, table):
    return spark.read.parquet(f"s3a://silver-layer/slv.{table}")

def read_gold_layer(spark, table):
    return spark.read.format("delta").load(f"s3a://gold-layer/gld.{table}")

def write_to_fact_orders(spark, gold_path):
    orders = read_silver_layer(spark, table="orders")
    order_details = read_silver_layer(spark, table="order_details")
    dim_products = read_gold_layer(spark, table="dim_products")
    dim_stores = read_gold_layer(spark, table="dim_stores")
    dim_payment = read_gold_layer(spark, table="dim_payment_method")

    if check_minio_has_data(bucket="gold-layer", prefix="gld.fact_order"):
        logger.info("[GOLD][gld.fact_orders] Existing data detected ->  Checking for new records...")
        existing_df = read_gold_layer(spark, table="fact_orders")
        latest_time = existing_df.select(max("order_date")).first()[0]
        new_orders = orders.filter(to_date(col("timestamp")) > latest_time)
    else:
        logger.info("[GOLD][gld.fact_orders] Initial load: performing full load.")
        new_orders = orders

    if new_orders.isEmpty():
        # No new records
        logger.debug("[GOLD][gld.fact_orders] No new updates found — table is up to date.")

    else:
        # Select only the necessary columns to improve query performance
        # Rename columns to avoid ambiguous column names and ensure clarity
        logger.info(f"[GOLD][gld.fact_orders] Found new records. Preparing to processing and write...")

        new_orders = new_orders.selectExpr(
            "order_id AS o_order_id",
            "timestamp AS o_order_date",
            "customer_id AS o_customer_id",
            "store_id AS o_store_id",
            "payment_method_id AS o_payment_method_id",
            "year",
            "month",
            "day"
        )

        order_details = order_details.selectExpr(
            "order_id AS od_order_id",
            "product_id AS od_product_id",
            "quantity AS od_quantity",
            "subtotal AS od_subtotal",
        )

        dim_products = dim_products.filter("is_current = true").selectExpr(
            "product_key AS p_product_key",
            "product_id AS p_product_id"
        )

        dim_stores = dim_stores.filter("is_current = true").selectExpr(
            "store_key AS s_store_key",
            "store_id AS s_store_id"
        )

        dim_payment = dim_payment.filter("is_current = true").selectExpr(
            "method_key AS pm_method_key",
            "method_id AS pm_method_id"
        )

        join_df = new_orders.join(order_details, new_orders["o_order_id"] == order_details["od_order_id"], how="inner")
        join_df = join_df.join(broadcast(dim_products), join_df["od_product_id"] == dim_products["p_product_id"], how="inner")
        join_df = join_df.join(broadcast(dim_stores), join_df["o_store_id"] == dim_stores["s_store_id"], how="inner")
        join_df = join_df.join(broadcast(dim_payment), join_df["o_payment_method_id"] == dim_payment["pm_method_id"], how="inner")
        
        join_df = join_df.selectExpr(
            "year", "month", "day", "o_order_date AS order_date", "o_order_id AS order_id",
            "o_customer_id AS customer_id", "s_store_key AS store_key", "pm_method_key AS payment_method_key",
            "p_product_key AS product_key", "od_quantity AS quantity", "od_subtotal AS subtotal"
        )

        join_df.write \
            .format("delta") \
            .partitionBy("year", "month", "day") \
            .mode("append") \
            .save(f"{gold_path}/gld.fact_orders")
        
        logger.success("[GOLD][gld.fact_orders] Fact table successfully written to Gold Layer.")


def main():
    logger.info("[GOLD][gld.fact_orders] --- Start processing ---")
    try:
        spark = create_SparkSession(
            app_name="slv.orders+order_details→gld.fact_orders",
            enable_delta=True
        )
        write_to_fact_orders(spark, gold_path="s3a://gold-layer")
    except Exception as e:
        logger.error(f"Error during gold layer transformation: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()