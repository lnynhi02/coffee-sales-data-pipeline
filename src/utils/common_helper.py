import os

from pathlib import Path
from dotenv import load_dotenv
from minio import Minio

from src.utils.paths import CONFIGS_DIR

dotenv_path = CONFIGS_DIR / ".env"
load_dotenv(dotenv_path)

def get_mysql_config():
    return {
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "host": os.getenv("MYSQL_HOST"),
        "database": os.getenv("MYSQL_DATABASE"),
    }

def check_minio_has_data(bucket, prefix) -> bool:
    client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    return any(client.list_objects(bucket, prefix=prefix, recursive=True))

