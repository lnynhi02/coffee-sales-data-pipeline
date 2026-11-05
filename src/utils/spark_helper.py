from pyspark.sql import SparkSession

from src.utils.minio_helper import get_minio_config

minio = get_minio_config()

BASE_S3_CONFIG = {
    "spark.hadoop.fs.s3a.endpoint": f"http://{minio['endpoint']}",
    "spark.hadoop.fs.s3a.access.key": minio["access_key"],
    "spark.hadoop.fs.s3a.secret.key": minio["secret_key"],
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
}

DELTA_CONFIG = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}

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


