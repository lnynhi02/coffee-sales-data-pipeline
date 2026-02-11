from loguru import logger
import sys
import json
from pathlib import Path
from datetime import datetime, date

TODAY = date.today().strftime("%Y-%m-%d")
BASE_DIR = Path(__file__).resolve().parent.parent.parent
LOG_DIR = BASE_DIR / "logs" / TODAY
LOG_DIR.mkdir(parents=True, exist_ok=True)


def json_sink(message):
    r = message.record
    data = {
        "ts": r["time"].timestamp(),
        "level": r["level"].name,
        "service": r["extra"].get("service"),
        "event": r["extra"].get("event"),
        "order_id": r["extra"].get("order_id"),
        "message": r["message"],
        "meta": r["extra"].get("meta"),
    }

    log_file = message.record["extra"]["_log_file"]
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")


def patch_record(record):
    extra = record["extra"]

    nested = extra.pop("extra", None)
    if isinstance(nested, dict):
        for k, v in nested.items():
            extra.setdefault(k, v)

    extra.setdefault("event", None)
    extra.setdefault("order_id", None)
    extra.setdefault("meta", None)

    return record



def setup_logger(service_name, branch):
    logger.remove()

    logger.configure(patcher=patch_record)

    # Console
    logger.add(
        sys.stdout,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level:<7}</level> | "
            f"{service_name:<16} | "
            "order={extra[order_id]} | "
            "msg=\"{message}\" | "
            "meta={extra[meta]}"
        ),
        level="INFO",
        enqueue=True,
    )

    log_path = LOG_DIR / branch
    log_path.mkdir(parents=True, exist_ok=True)
    log_file = log_path / f"{service_name}.log"

    # JSON business log
    logger.add(
        json_sink, 
        level="INFO",
        enqueue=True,
    )

    return logger.bind(
        service=service_name,
        _log_file=str(log_file)
    )
