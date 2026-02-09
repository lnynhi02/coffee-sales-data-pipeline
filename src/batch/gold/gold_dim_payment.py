# ==================================================================================
# SCRIPT: Create Dimension Table 'gld.dim_payment'
# ==================================================================================
# Purpose:
#   - Create the dimension table 'gld.dim_payment' in the Gold Layer using Delta format
#   - Implement Slowly Changing Dimension (SCD) Type 2 logic to track historical changes
#
# Usage:
#   - Can be queried directly for analytics and reporting.
# =================================================================================
import sys
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(BASE_DIR)

from loguru import logger

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable

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


def initial_load_dim_payment_method(source_df, gld_dim_payment_method):
    output_df = source_df.withColumn("temp_id", monotonically_increasing_id()) \
                        .withColumn("method_key", col("temp_id").cast("long")) \
                        .withColumn("start_date", col("updated_at")) \
                        .withColumn("end_date", lit(None).cast("date")) \
                        .withColumn("is_current", lit(True)) \
                        .drop("temp_id")
    
    output_df = output_df.select(
        "method_key", "method_id", "method_name", "bank", "start_date", "end_date", "is_current"
    )

    output_df.write.format("delta").mode("overwrite").save(gld_dim_payment_method)
    logger.success("[GOLD][gld.dim_payment_method] Initial load completed — data written successfully.")


def perform_scd2(spark: SparkSession, source_df, gld_dim_payment_method):
    target_df = spark.read.format("delta").load(gld_dim_payment_method)
    latest_time = target_df.select(max("start_date")).first()[0]
    new_data = source_df.filter(col("updated_at") > latest_time)

    if new_data.isEmpty():
        logger.debug("[GOLD][gld.dim_payment_method] No new updates found — table is up to date.")
    else:
        logger.info("[GOLD][gld.dim_payment_method] New changes detected in source — proceeding with SCD Type 2 merge.")        

        join_df = source_df.join(
            target_df,
            (source_df["method_id"] == target_df["method_id"]) & (target_df["is_current"] == True),
            how="left"
        ).select(
            source_df["*"],
            target_df.method_id.alias("target_method_id"),
            target_df.method_name.alias("target_method_name"),
            target_df.bank.alias("target_bank")
        )

        changes_df = join_df.filter(xxhash64(col("method_id"), col("method_name"), col("bank")) !=
                                    xxhash64(col("target_method_id"), col("target_method_name"), col("target_bank")))

        record_to_merge = changes_df.withColumn("merge_key", col("method_id"))

        # Filter the records that are in the table and need to be updated
        record_to_update = changes_df.filter(col("target_method_id").isNotNull()) \
                                    .withColumn("merge_key", lit(None).cast("int"))
        
        final_merge_df = record_to_merge.union(record_to_update)
        delta_table = DeltaTable.forPath(spark, gld_dim_payment_method)
        try:
            current_df = delta_table.toDF()
            max_key_row = current_df.agg({"method_key": "max"}).collect()[0][0]
            max_key = max_key_row if max_key_row is not None else 0
        except Exception:
            max_key = 0

        update_and_insert = final_merge_df.withColumn(
            "temp_id", monotonically_increasing_id()
        ).withColumn(
            "method_key", (col("temp_id") + lit(max_key)).cast("long")
        ).drop("temp_id")

        delta_table.alias("target").merge(
            update_and_insert.alias("update"),
            condition="target.method_id == update.merge_key AND target.is_current == True"
        ).whenMatchedUpdate(set={
            "is_current": lit(False),
            "end_date": current_date()
        }).whenNotMatchedInsert(values={
            "method_key": "update.method_key",
            "method_id": "update.method_id",
            "method_name": "update.method_name",
            "bank": "update.bank",
            "start_date": "update.updated_at",
            "end_date": lit(None),
            "is_current": lit(True)
        }).execute()

        logger.success("[GOLD][gld.dim_payment_method] Merge completed successfully (SCD Type 2 applied).")


def main():
    logger.info("[GOLD][gld.dim_payment_method] --- Start processing ---")

    try:
        spark = create_SparkSession(
            app_name="slv.payment_method→gld.dim_payment_method",
            enable_delta=True
        )

        source_df = spark.read.parquet("s3a://silver-layer/slv.payment_method")
        gld_dim_payment_method = "s3a://gold-layer/gld.dim_payment_method"

        if check_minio_has_data(bucket="gold-layer", prefix="gld.dim_payment_method/"):
            logger.info("[GOLD][gld.dim_payment_method] Existing data detected ->  Checking for new records...")
            perform_scd2(spark, source_df, gld_dim_payment_method)
        else:
            initial_load_dim_payment_method(source_df, gld_dim_payment_method)

    except Exception as e:
        logger.error(f"Error during gold layer transformation: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()