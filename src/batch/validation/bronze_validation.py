# ==================================================================================
# SCRIPT: Validate Bronze Layer Data Quality and Schema using PySpark
# ==================================================================================
# Purpose:
#   - Perform schema validation against stored schema definitions for Bronze tables.
#   - Check data quality rules: nullability and uniqueness constraints per table.
#   - Log issues and updates for monitoring and debugging.
# ==================================================================================

import sys
import json
from loguru import logger
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Logging setup
LOG_FILE = BASE_DIR / "logger" / "data_validation.log"
logger.remove()

logger.add(
    LOG_FILE, 
    rotation="00:00",
    encoding="utf-8",
    enqueue=True
)

# Spark session setup
spark = SparkSession.builder \
    .appName("Bronze Layer Data Quality Check") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.warehouse.dir", str(BASE_DIR / "tmp" / "sql_warehouse")) \
    .config("spark.local.dir", str(BASE_DIR / "tmp" / "local_dir")) \
    .getOrCreate()

# Schema file location
SCHEMA_FILE = Path(__file__).resolve().parent / "bronze_schema.json"

def read_schema_file():
    if SCHEMA_FILE.exists():
        with open(SCHEMA_FILE) as f:
            return json.load(f)
    return {}


def validate_schema(df, table_name: str, schema_store: dict) -> bool:
    current_schema = json.loads(df.schema.json())
    expected_schema = schema_store[table_name]
 
    if current_schema != expected_schema:
        logger.warning(f"[{table_name}] Schema mismatch detected.")
        logger.debug(f"[{table_name}] Expected schema: {json.dumps(expected_schema, indent=2)}")
        logger.debug(f"[{table_name}] Current schema : {json.dumps(current_schema, indent=2)}")
        return False

    logger.info(f"[{table_name}] Schema check passed.")
    return True


def check_table_quality(df, table_name: str, null_cols=[], unique_cols=[]) -> bool:
    has_issue = False

    for col in null_cols:
        null_count = df.filter(f"{col} IS NULL").count()
        if null_count > 0.05 * df.count():
            logger.warning(f"[{table_name}] {null_count} NULL values found in column '{col}'")
            has_issue = True

    for col in unique_cols:
        dup_count = df.groupBy(col).count().filter("count > 1").count()
        if dup_count > 0:
            logger.warning(f"[{table_name}] {dup_count} duplicate values found in column '{col}'")
            has_issue = True

    if not has_issue:
        logger.info(f"[{table_name}] Null and uniqueness checks passed.")
    return has_issue


def main():
    path = "s3a://bronze-layer"

    tables = {
        "brz.orders": {
            "df": spark.read.parquet(f"{path}/brz.orders"),
            "null_cols": ["id", "customer_id", "payment_method_id", "store_id"],
            "unique_cols": ["id"]
        },
        "brz.order_details": {
            "df": spark.read.parquet(f"{path}/brz.order_details"),
            "null_cols": ["order_id", "product_id", "quantity", "subtotal"],
            "unique_cols": []
        },
        "brz.order_suggestion_accepted": {
            "df": spark.read.parquet(f"{path}/brz.order_suggestion_accepted"),
            "null_cols": ["order_id", "product_id", "quantity", "subtotal"],
            "unique_cols": []
        },
        "brz.products": {
            "df": spark.read.parquet(f"{path}/brz.products"),
            "null_cols": ["id", "name", "category_id", "unit_price", "updated_at"],
            "unique_cols": ["id"]
        },
        "brz.product_category": {
            "df": spark.read.parquet(f"{path}/brz.product_category"),
            "null_cols": ["id", "updated_at"],
            "unique_cols": ["id"]
        },
        "brz.stores": {
            "df": spark.read.parquet(f"{path}/brz.stores"),
            "null_cols": ["id", "address", "district", "city", "updated_at"],
            "unique_cols": ["id"]
        }
    }

    schema_store = read_schema_file()
    schema_changed = False
    found_issue = False

    for table_name, config in tables.items():
        df = config["df"]
        logger.info(f"Validating table: {table_name}")

        if table_name not in schema_store:
            schema_store[table_name] = json.loads(df.schema.json())
            logger.info(f"[{table_name}] Initial schema saved.")
            schema_changed = True
        elif not validate_schema(df, table_name, schema_store):
            schema_changed = True
            found_issue = True

        if check_table_quality(df, table_name, config["null_cols"], config["unique_cols"]):
            found_issue = True

    if schema_changed:
        with open(SCHEMA_FILE, "w") as f:
            json.dump(schema_store, f, indent=2)
        logger.info("Schema file updated.")

    if not found_issue:
        logger.info("All Bronze checks passed.")
    else:
        logger.warning("Bronze validation completed with issues.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Validation error: {e}")
        sys.exit(1)
    finally:
        spark.stop()
