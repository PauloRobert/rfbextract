import json
import logging
import os
import sys
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "etl.log")
MAX_LOG_SIZE = int(os.getenv("MAX_LOG_SIZE", 10485760))  # 10MB
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", 5))

LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }

        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        for key, value in record.__dict__.items():
            if key not in ['args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename',
                           'funcName', 'id', 'levelname', 'levelno', 'lineno', 'module',
                           'msecs', 'message', 'msg', 'name', 'pathname', 'process',
                           'processName', 'relativeCreated', 'stack_info', 'thread', 'threadName']:
                log_record[key] = value

        return json.dumps(log_record)

class ETLLogger:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ETLLogger, cls).__new__(cls)
            cls._instance._setup_logger()
        return cls._instance

    def _setup_logger(self):
        self.logger = logging.getLogger("etl_pipeline")
        self.logger.setLevel(LOG_LEVELS.get(LOG_LEVEL, logging.INFO))

        self.logger.handlers = []

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(JsonFormatter())
        self.logger.addHandler(console_handler)

        if LOG_FILE:
            log_dir = Path(LOG_FILE).parent
            log_dir.mkdir(parents=True, exist_ok=True)

            file_handler = RotatingFileHandler(
                LOG_FILE,
                maxBytes=MAX_LOG_SIZE,
                backupCount=LOG_BACKUP_COUNT
            )
            file_handler.setFormatter(JsonFormatter())
            self.logger.addHandler(file_handler)

    def info(self, message, **kwargs):
        self.logger.info(message, extra=kwargs)

    def warning(self, message, **kwargs):
        self.logger.warning(message, extra=kwargs)

    def error(self, message, exception=None, **kwargs):
        if exception:
            kwargs["exception_type"] = type(exception).__name__
            kwargs["exception_msg"] = str(exception)
            self.logger.error(message, exc_info=exception, extra=kwargs)
        else:
            self.logger.error(message, extra=kwargs)

    def critical(self, message, exception=None, **kwargs):
        if exception:
            kwargs["exception_type"] = type(exception).__name__
            kwargs["exception_msg"] = str(exception)
            self.logger.critical(message, exc_info=exception, extra=kwargs)
        else:
            self.logger.critical(message, extra=kwargs)

    def debug(self, message, **kwargs):
        self.logger.debug(message, extra=kwargs)

    def metric(self, name, value, **kwargs):
        self.logger.info(f"METRIC: {name} = {value}", extra={
            "metric_name": name,
            "metric_value": value,
            **kwargs
        })

    def start_timer(self, name):
        start_time = time.time()
        self.logger.debug(f"START TIMER: {name}", extra={
            "timer_name": name,
            "timer_start": start_time
        })
        return start_time

    def end_timer(self, name, start_time):
        end_time = time.time()
        duration = end_time - start_time
        self.logger.info(f"END TIMER: {name} took {duration:.2f} seconds", extra={
            "timer_name": name,
            "timer_duration": duration,
            "timer_unit": "seconds"
        })
        return duration

    def progress(self, current, total, operation):
        percentage = (current / total) * 100 if total > 0 else 0
        self.logger.info(f"PROGRESS: {operation} - {current}/{total} ({percentage:.2f}%)", extra={
            "progress_operation": operation,
            "progress_current": current,
            "progress_total": total,
            "progress_percentage": percentage
        })

logger = ETLLogger()