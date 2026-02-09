import json
import logging
from pathlib import Path
from datetime import datetime, date

TODAY = date.today().strftime("%Y-%m-%d")
BASE_DIR = Path(__file__).resolve().parent.parent.parent
LOG_DIR = BASE_DIR / "logs" / TODAY
LOG_DIR.mkdir(parents=True, exist_ok=True)


class JsonFormatter(logging.Formatter):
    def format(self, record):
        base = {
            "timestamp": datetime.now().isoformat(),
            "level": record.levelname,
            "service": record.name,
            "event": getattr(record, "event", None),
            "order_id": getattr(record, "order_id", None),
            "message": record.getMessage(),
            "meta": getattr(record, "meta", None)
        }
        return json.dumps(base, ensure_ascii=False)


class ConsoleFormatter(logging.Formatter):
    def format(self, record):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return (
            f"{ts} | {record.levelname:<5} | "
            f"{record.name:<16} | "
            f"order={getattr(record, 'order_id', '')} | "
            f"msg=\"{record.getMessage()}\" | "
            f"meta={getattr(record, 'meta', None)}"
        )


def get_logger(service_name, branch):
    logger = logging.getLogger(service_name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        # Console handler
        ch = logging.StreamHandler()
        ch.setFormatter(ConsoleFormatter())
        logger.addHandler(ch)

        # File handler
        log_path = LOG_DIR / branch
        log_path.mkdir(parents=True, exist_ok=True)

        file_name = f"{service_name}.log"
        fh = logging.FileHandler(str(log_path / file_name))
        fh.setFormatter(JsonFormatter())
        logger.addHandler(fh)

    return logger