# ==================================================================================
# SCRIPT: Create Dimension Table 'gld.dim_stores'
# ==================================================================================
# Purpose:
#   - Create the dimension table 'gld.dim_stores' in the Gold Layer using Delta format
#   - Implement Slowly Changing Dimension (SCD) Type 2 logic to track historical changes
#
# Usage:
#   - Can be queried directly for analytics and reporting.
# ==================================================================================
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


def inital_load_dim_stores(source_df, gld_dim_stores) -> None:
    output_df = source_df.withColumn("temp_id", monotonically_increasing_id()) \
                        .withColumn("store_key", col("temp_id").cast("long")) \
                        .withColumn("start_date", col("updated_at")) \
                        .withColumn("end_date", lit(None).cast("date")) \
                        .withColumn("is_current", lit(True))
    
    output_df = output_df.select(
        "store_key", "store_id", "store_name", "address", "district", "city", 
        "start_date", "end_date", "is_current"
    )
    output_df.write.format("delta").mode("overwrite").save(gld_dim_stores)


def perform_scd2(spark: SparkSession, source_df, gld_dim_stores) -> None:
    target_df = spark.read.format("delta").load(gld_dim_stores)
    latest_time = target_df.select(max("start_date")).first()[0]
    new_data = source_df.filter(col("updated_at") > latest_time)

    if new_data.isEmpty():
        logger.debug("[GOLD][gld.dim_stores] No new updates found — up to date.")
    else:
        logger.info("[GOLD][gld.dim_stores] New changes detected — proceeding with SCD Type 2 merge.")

        join_df = source_df.join(
            target_df,
            (source_df["store_id"] == target_df["store_id"]) & (target_df["is_current"] == True)
        ).select(
            source_df["*"],
            target_df.store_id.alias("target_store_id"),
            target_df.store_name.alias("target_store_name"),
            target_df.address.alias("target_address"),
            target_df.district.alias("target_district"),
            target_df.city.alias("target_city"),
        )

        changes_df = join_df.filter(xxhash64(col("store_id"), col("store_name"), col("address"), col("district"), col("city")) !=
                                    xxhash64(col("target_store_id"), col("target_store_name"), col("target_address"), col("target_district"), col("target_city")))

        record_to_merge = changes_df.withColumn("merge_key", col("store_id"))

        # Filter records that are in the table and need to be updated
        record_to_update = changes_df.filter(col("target_store_id").isNotNull()) \
                                    .withColumn("merge_key", lit(None).cast("int"))

        final_merge_df = record_to_merge.union(record_to_update)
        delta_table = DeltaTable.forPath(spark, gld_dim_stores)
        try:
            max_key_row = delta_table.toDF().agg({"store_key": "max"}).collect()[0][0]
            max_key = max_key_row if max_key_row is not None else 0
        except Exception:
            max_key = 0

        update_and_insert = final_merge_df.withColumn("temp_id", monotonically_increasing_id()) \
            .withColumn("store_key", (col("temp_id") + lit(max_key)).cast("long")) \
            .drop("temp_id")
        
        delta_table.alias("target").merge(
            update_and_insert.alias("update"),
            condition="target.store_id == update.merge_key AND target.is_current == True"
        ).whenMatchedUpdate(set={
            "is_current": lit(False),
            "end_date": current_date()
        }).whenNotMatchedInsert(values={
            "store_key": "update.store_key",
            "store_id": "update.store_id",
            "store_name": "update.store_name",
            "address": "update.address",
            "district": "update.district",
            "city": "udpate.city",
            "start_date": "update.updated_at",
            "end_date": lit(None),
            "is_current": lit(True)
        }).execute()

        logger.success("[GOLD][gld.dim_stores] Merge completed successfully (SCD Type 2 applied).")


def main():
    logger.info("[GOLD][gld.dim_stores] --- Start processing ---")
    spark = create_SparkSession(
            app_name="slv.stores→gld.dim_stores",
            enable_delta=True
        )
    
    try:
        source_df = spark.read.parquet("s3a://silver-layer/slv.stores")
        gld_dim_stores = "s3a://gold-layer/gld.dim_stores"
        
        if check_minio_has_data(bucket="gold-layer", prefix="gld.dim_stores/"):
            logger.info("[GOLD][gld.dim_stores] Existing data detected ->  Checking for new records...")
            perform_scd2(spark, source_df, gld_dim_stores)
        else:
            inital_load_dim_stores(source_df, gld_dim_stores)

    except Exception as e:
        logger.error(f"Error during gold layer transformation: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()