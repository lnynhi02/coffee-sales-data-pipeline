# ================================================================
# SCRIPT: Load data from MySQL to Bronze Layer (Raw Zone)
# ================================================================
# Purpose:
#   - Extracts data from MySQL source tables.
#   - If Bronze data does NOT exist in MinIO (Parquet format):
#       → Perform full load from MySQL to Bronze.
#   - If data EXISTS:
#       → Perform incremental load (only new records).
# ================================================================

import os
import sys
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(BASE_DIR)

from loguru import logger

from pyspark.sql import SparkSession
from pyspark.sql.types import *
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


def read_mysql_table(spark: SparkSession, table: str):
    host = os.getenv("MYSQL_HOST")
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    database = os.getenv("MYSQL_DATABASE")

    return spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{host}:3306/{database}?user={user}&password={password}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", table) \
        .load()


def incremental_load_orders(spark: SparkSession) -> None:
    try:
        logger.info(
            "[BRONZE][orders] --- Start processing orders and order_details ---"
        )
        orders_df = read_mysql_table(spark, "orders")
        details_df = read_mysql_table(spark, "order_details")

        orders_path = "s3a://bronze-layer/brz.orders"
        details_path = "s3a://bronze-layer/brz.order_details"

        # If Bronze data exists in MinIO, perform incremental load; otherwise, do full load
        if check_minio_has_data(bucket="bronze-layer", prefix="brz.orders/"):
            logger.info(
                "[BRONZE][orders] Existing Bronze data found -> Checking for new records."
            )
            existing_orders = spark.read.parquet(orders_path)
            latest_time = existing_orders.select(max("timestamp")).first()[0]
            new_orders = orders_df.filter(col("timestamp") > latest_time)
        else:
            logger.debug(
                "[BRONZE][orders] No Bronze data found -> Performing full load."
            )
            new_orders = orders_df

        if new_orders.isEmpty():
            logger.debug("[BRONZE][orders] No new records to write. Skipping.")
            return

        logger.debug(f"[BRONZE][orders] Found new records. Preparing to write...")

        enriched_orders = new_orders.withColumn("year", year("timestamp")) \
                                    .withColumn("month", month("timestamp")) \
                                    .withColumn("day", dayofmonth("timestamp"))

        joined_details = enriched_orders.join(
            details_df, enriched_orders["id"] == details_df["order_id"], "inner"
        ).select(
            "order_id",
            "product_id",
            "quantity",
            "discount_percent",
            "subtotal",
            "is_suggestion",
            "year",
            "month",
            "day",
        )

        logger.info("[BRONZE][orders] Writing new orders and order_details to Bronze...")
        enriched_orders.write.partitionBy("year", "month", "day").mode("append").parquet(orders_path)
        joined_details.write.partitionBy("year", "month", "day").mode("append").parquet(details_path)

        logger.success("[BRONZE][orders] Successfully wrote orders and order_details.")
    except Exception as e:
        logger.error(f"[BRONZE][orders] Error during load: {str(e)}")
        raise


def incremental_load_order_with_suggestion(spark: SparkSession) -> None:
    try:
        logger.info(
            "[BRONZE][order_suggestion_accepted] --- Start processing accepted suggested products ---"
        )

        bootstrap_servers = ["kafka-1:9092", "kafka-2:9092"]
        bootstrap_servers_str = ",".join(bootstrap_servers)

        order_with_suggestion = spark.read \
                                .format("kafka") \
                                .option("kafka.bootstrap.servers", bootstrap_servers_str) \
                                .option("subscribe", "order_suggestion_accepted") \
                                .option("startingOffsets", "earliest") \
                                .load()
        
        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("quantity", IntegerType(), True),
            StructField("discount_percent", IntegerType(), True),
            StructField("subtotal", IntegerType(), True),
            StructField("is_suggestion", BooleanType(), True)
        ])
        
        order_with_suggestion = order_with_suggestion.selectExpr("CAST(value AS STRING)") \
                                    .select(from_json(col("value"), schema).alias("data")) \
                                    .select("data.*")
        
        order_with_suggestion = order_with_suggestion \
                                .withColumn("suggest_date", current_date()) \
                                .withColumn("year", year("suggest_date")) \
                                .withColumn("month", month("suggest_date")) \
                                .withColumn("day", dayofmonth("suggest_date")) \
                                .drop("suggest_date")
                                        
        order_with_suggestion.write.partitionBy("year", "month", "day").mode("append").parquet("s3a://bronze-layer/brz.order_suggestion_accepted")
        logger.success("[BRONZE][order_suggestion_accepted] --- Successfully wrote to Bronze ---")
    except Exception as e:
        logger.error(f"[BRONZE][order_suggestion_accepted] Error during load: {e}")
        raise


def incremental_load_dimension(
    spark: SparkSession, table_name: str, time_col="updated_at"
) -> None:
    bronze_path = f"s3a://bronze-layer/brz.{table_name}"
    try:
        logger.info(f"[BRONZE][{table_name}] --- Start processing ---")
        source_df = read_mysql_table(spark, table_name)

        # If Bronze data exists in MinIO, perform incremental load; otherwise, do full load
        if check_minio_has_data(bucket="bronze-layer", prefix=f"brz.{table_name}/"):
            logger.info(
                f"[BRONZE][{table_name}] Existing Bronze data found -> Checking for new records."
            )
            existing_df = spark.read.parquet(bronze_path)
            latest_time = existing_df.select(max(time_col)).first()[0]
            new_data = source_df.filter(col(time_col) > latest_time)
        else:
            logger.debug(
                f"[BRONZE][{table_name}] No Bronze data found -> Performing full load."
            )
            new_data = source_df

        if new_data.rdd.isEmpty():
            logger.debug(f"[BRONZE][{table_name}] No new records to write. Skipping.")
        else:
            output_df = new_data.withColumn("year", year(col(time_col))) \
                                .withColumn("month", month(col(time_col))) \
                                .withColumn("day", dayofmonth(col(time_col)))

            logger.info(f"[BRONZE][{table_name}] Writing {output_df.count()} new records to Bronze...")
            output_df.write.partitionBy("year", "month", "day").mode("append").parquet(bronze_path)
            logger.success(f"[BRONZE][{table_name}] Successfully wrote to Bronze.")
    except Exception as e:
        logger.error(f"[BRONZE][{table_name}] Error during load: {str(e)}")
        raise


def main():
    logger.info("======== [BRONZE] START: Loading MySQL data to Bronze Layer ========")

    try:
        spark = create_SparkSession(app_name="mysql+kafka→bronze.minio")

        incremental_load_orders(spark)
        incremental_load_order_with_suggestion(spark)

        dimension_tables = [
            "stores",
            "product_category",
            "products",
            "payment_method",
            "customers",
        ]
        for table in dimension_tables:
            incremental_load_dimension(spark, table)

        spark.stop()
        logger.info("======== [BRONZE] COMPLETED ========")

    except:
        sys.exit(1)


if __name__ == "__main__":
    main()
