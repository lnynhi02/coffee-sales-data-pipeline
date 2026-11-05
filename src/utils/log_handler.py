import sys
import json

from loguru import logger
from pathlib import Path
from datetime import datetime, date

from src.utils.paths import LOG_DIR

TODAY = date.today().strftime("%Y-%m-%d")
LOG_DIR = LOG_DIR / TODAY
LOG_DIR.mkdir(parents=True, exist_ok=True)


def json_sink(log_file: str, message) -> None:
    record = message.record
    data = {
        "ts": record["time"].isoformat(),
        "level": record["level"].name,
        "service": record["extra"].get("service"),
        "event": record["extra"].get("event"),
        "component": record["extra"].get("component"),
        "request_id": record["extra"].get("request_id"),
        "order_id": record["extra"].get("order_id"),
        "message": record["message"],
        "meta": record["extra"].get("meta"),
        "exception": (
            record["exception"].message if record["exception"] else None
        ),
    }

    with open(log_file, "a", encoding="utf-8") as file:
        file.write(json.dumps(data, ensure_ascii=False, default=str) + "\n")


def patch_record(record: dict) -> dict:
    extra = record["extra"]

    nested = extra.pop("extra", None)
    if isinstance(nested, dict):
        for k, v in nested.items():
            extra.setdefault(k, v)

    extra.setdefault("service", None)
    extra.setdefault("event", None)
    extra.setdefault("component", None)
    extra.setdefault("request_id", None)
    extra.setdefault("order_id", None)
    extra.setdefault("meta", None)

    return record


def setup_logger(service: str, component: str):
    logger.remove()

    logger.configure(patcher=patch_record)

    # Console
    logger.add(
        sys.stdout,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level:<7}</level> | "
            f"{service:<16} | {component:<24} | "
            "order={extra[order_id]} | "
            "msg=\"{message}\" | "
            "meta={extra[meta]}"
        ),
        level="INFO",
        enqueue=True,
    )

    log_path = LOG_DIR / service
    log_path.mkdir(parents=True, exist_ok=True)
    log_file = log_path / f"{component}.jsonl"

    # JSON business log
    logger.add(
        lambda message: json_sink(str(log_file), message),
        level="INFO",
        enqueue=True,
    )

    return logger.bind(
        service=service,
        component=component,
    )
