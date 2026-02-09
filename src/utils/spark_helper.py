from pathlib import Path
from dotenv import load_dotenv

from minio import Minio
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parent.parent.parent
dotenv_path = Path("configs/.env")
load_dotenv(dotenv_path)

BASE_S3_CONFIG = {
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
}

DELTA_CONFIG = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


def check_minio_has_data(bucket, prefix) -> bool:
    client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    return any(client.list_objects(bucket, prefix=prefix, recursive=True))

def create_SparkSession(app_name: str, enable_delta: bool = False, extra_conf: dict[str, str] = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)

    for key, val in BASE_S3_CONFIG.items():
        builder = builder.config(key, val)

    if enable_delta:
        for key, val in DELTA_CONFIG.items():
            builder = builder.config(key, val)

    if extra_conf:
        for key, val in extra_conf.items():
            builder = builder.config(key, val)

    return builder.getOrCreate()