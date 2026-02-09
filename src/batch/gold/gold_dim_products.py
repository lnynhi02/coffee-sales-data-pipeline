# ==================================================================================
# SCRIPT: Create Dimension Table 'gld.dim_products'
# ==================================================================================
# Purpose:
#   - Create the dimension table 'gld.dim_products' in the Gold Layer using Delta format
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


def inital_load_dim_products(source_df, gld_dim_products):
    output_df = source_df.withColumn("temp_id", monotonically_increasing_id()) \
        .withColumn("product_key", col("temp_id").cast("long")) \
        .drop("temp_id") \
        .withColumn("start_date", col("updated_at")) \
        .withColumn("end_date", lit(None).cast("date")) \
        .withColumn("is_current", lit(True).cast("string"))
        
    output_df.select(
        "product_key", "product_id", "product_name", 
        "category", "unit_price", "start_date", "end_date", "is_current") \
        .write.format("delta").mode("overwrite").save(gld_dim_products)
    
    logger.success("[GOLD][gld.dim_products] Initial load completed - data written successfully.")


def perform_scd2(spark: SparkSession, source_df, gld_dim_products):
    target_df = spark.read.format("delta").load(gld_dim_products)
    latest_date = target_df.select(max("start_date")).first()[0]
    new_data = source_df.filter(col("updated_at") > latest_date)

    if new_data.isEmpty():
        logger.debug("[GOLD][gld.dim_products] No new updates found — up to date.")
    else:
        logger.info("[GOLD][gld.dim_products] New changes detected in source — proceeding with SCD Type 2 merge.")
        join_df = source_df.join(
            target_df,
            (source_df["product_id"] == target_df["product_id"]) & (target_df["is_current"] == True),
            how="left" 
        ).select(
            source_df["*"],
            target_df.product_id.alias("target_product_id"),
            target_df.product_name.alias("target_product_name"),
            target_df.category.alias("target_category"),
            target_df.unit_price.alias("target_unit_price"),
            target_df.start_date
        )

        # Filter only the records that are updated and inserted
        changes_df = join_df.filter(xxhash64(col("product_name"), col("category"), col("unit_price")) != 
                                    xxhash64(col("target_product_name"), col("target_category"), col("target_unit_price")))
        records_to_update_insert = changes_df.withColumn("merge_key", col("product_id"))
        
        # Filter only records that need update and already in the table
        records_to_update = changes_df.filter(col("target_product_id").isNotNull()) \
                                      .withColumn("merge_key", lit(None).cast("int"))
        
        final_merge_df = records_to_update_insert.union(records_to_update)
        # Generate product_key (surrogate key)
        delta_table = DeltaTable.forPath(spark, gld_dim_products)
        try:
            current_df = delta_table.toDF()
            max_key_row = current_df.agg({"product_key": "max"}).collect()[0][0]
            max_key = max_key_row if max_key_row is not None else 0
        except Exception:
            max_key = 0

        update_and_insert = final_merge_df.withColumn(
            "temp_id", monotonically_increasing_id()
        ).withColumn(
            "product_key", (col("temp_id") + lit(max_key)).cast("long")
        ).drop("temp_id")

        delta_table.alias("target").merge(
            update_and_insert.alias("update"),
            condition="target.product_id == update.merge_key AND target.is_current == True"
        ).whenMatchedUpdate(set={
            "is_current": lit(False),
            "end_date": current_date()
        }).whenNotMatchedInsert(values={
            "product_key": "update.product_key",
            "product_id": "update.product_id",
            "product_name": "update.product_name",
            "category": "update.category",
            "unit_price": "update.unit_price",
            "start_date": "update.updated_at",
            "end_date": lit(None),
            "is_current": lit(True)
        }).execute()

        logger.success("[GOLD][gld.dim_products] Merge completed successfully (SCD Type 2 applied).")


def main():
    logger.info("[GOLD][gld.dim_payment] --- Start processing ---")

    try:
        spark = create_SparkSession(
            app_name="slv.products→gld.dim_products",
            enable_delta=True
        )
        
        source_df = spark.read.parquet("s3a://silver-layer/slv.products")
        gld_dim_products = "s3a://gold-layer/gld.dim_products"

        if check_minio_has_data(bucket="gold-layer", prefix="gld.dim_products/"):
            logger.info("[GOLD][gld.dim_products] Existing data detected ->  Checking for new records...")
            perform_scd2(spark, source_df, gld_dim_products)
        else:
            inital_load_dim_products(source_df, gld_dim_products)
        
    except Exception as e:
        logger.error(f"Error during gold layer transformation: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()

        
if __name__ == "__main__":
    main()
