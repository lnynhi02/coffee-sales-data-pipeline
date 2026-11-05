import os

from dotenv import load_dotenv
from minio import Minio

from src.utils.paths import ENV_FILE

load_dotenv(ENV_FILE)

def get_minio_config() -> dict[str, str | bool]:
    required = ("MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY")
    missing = [name for name in required if not os.getenv(name)]

    if missing:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}"
        )
    
    return {
        "endpoint": os.getenv("MINIO_ENDPOINT"),
        "access_key": os.getenv("MINIO_ACCESS_KEY"),
        "secret_key": os.getenv("MINIO_SECRET_KEY"),
    }


def check_minio_has_data(bucket, prefix) -> bool:
    minio_config = get_minio_config()
    
    client = Minio(
        endpoint=minio_config["endpoint"],
        access_key=minio_config["access_key"],
        secret_key=minio_config["secret_key"],
        secure=False
    )

    return any(client.list_objects(bucket, prefix=prefix, recursive=True))
