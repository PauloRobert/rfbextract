import json
import logging
import os
import sys
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, Any, Optional

from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configurações de log
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "etl.log")
MAX_LOG_SIZE = int(os.getenv("MAX_LOG_SIZE", 10485760))  # 10MB
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", 5))

# Mapear strings de nível de log para constantes do logging
LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}

class JsonFormatter(logging.Formatter):
    """
    Formata logs como JSON para facilitar a análise e agregação.
    """
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

        # Adicionar exceções se existirem
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        # Adicionar atributos extras
        for key, value in record.__dict__.items():
            if key not in ['args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename',
                           'funcName', 'id', 'levelname', 'levelno', 'lineno', 'module',
                           'msecs', 'message', 'msg', 'name', 'pathname', 'process',
                           'processName', 'relativeCreated', 'stack_info', 'thread', 'threadName']:
                log_record[key] = value

        return json.dumps(log_record)

class ETLLogger:
    """
    Gerenciador centralizado de logs para o pipeline de ETL.
    Oferece métodos para registrar eventos, métricas e erros.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ETLLogger, cls).__new__(cls)
            cls._instance._setup_logger()
        return cls._instance

    def _setup_logger(self):
        """Configura o logger com formatação JSON e rotação de arquivos."""
        self.logger = logging.getLogger("etl_pipeline")
        self.logger.setLevel(LOG_LEVELS.get(LOG_LEVEL, logging.INFO))

        # Limpar handlers existentes
        self.logger.handlers = []

        # Configurar handler para console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(JsonFormatter())
        self.logger.addHandler(console_handler)

        # Configurar handler para arquivo com rotação
        if LOG_FILE:
            # Garantir que o diretório de logs exista
            log_dir = Path(LOG_FILE).parent
            log_dir.mkdir(parents=True, exist_ok=True)

            file_handler = RotatingFileHandler(
                LOG_FILE,
                maxBytes=MAX_LOG_SIZE,
                backupCount=LOG_BACKUP_COUNT
            )
            file_handler.setFormatter(JsonFormatter())
            self.logger.addHandler(file_handler)

    def info(self, message: str, **kwargs):
        """Registra uma mensagem informativa com metadados opcionais."""
        self.logger.info(message, extra=kwargs)

    def warning(self, message: str, **kwargs):
        """Registra um aviso com metadados opcionais."""
        self.logger.warning(message, extra=kwargs)

    def error(self, message: str, exception: Optional[Exception] = None, **kwargs):
        """Registra um erro com detalhes da exceção e metadados opcionais."""
        if exception:
            kwargs["exception_type"] = type(exception).__name__
            kwargs["exception_msg"] = str(exception)
            self.logger.error(message, exc_info=exception, extra=kwargs)
        else:
            self.logger.error(message, extra=kwargs)

    def critical(self, message: str, exception: Optional[Exception] = None, **kwargs):
        """Registra um erro crítico com detalhes da exceção e metadados opcionais."""
        if exception:
            kwargs["exception_type"] = type(exception).__name__
            kwargs["exception_msg"] = str(exception)
            self.logger.critical(message, exc_info=exception, extra=kwargs)
        else:
            self.logger.critical(message, extra=kwargs)

    def debug(self, message: str, **kwargs):
        """Registra uma mensagem de depuração com metadados opcionais."""
        self.logger.debug(message, extra=kwargs)

    def metric(self, name: str, value: Any, **kwargs):
        """Registra uma métrica de performance ou progresso."""
        self.logger.info(f"METRIC: {name} = {value}", extra={
            "metric_name": name,
            "metric_value": value,
            **kwargs
        })

    def start_timer(self, name: str) -> float:
        """Inicia um temporizador para medir a duração de uma operação."""
        start_time = time.time()
        self.logger.debug(f"START TIMER: {name}", extra={
            "timer_name": name,
            "timer_start": start_time
        })
        return start_time

    def end_timer(self, name: str, start_time: float):
        """Finaliza um temporizador e registra a duração."""
        end_time = time.time()
        duration = end_time - start_time
        self.logger.info(f"END TIMER: {name} took {duration:.2f} seconds", extra={
            "timer_name": name,
            "timer_duration": duration,
            "timer_unit": "seconds"
        })
        return duration

    def progress(self, current: int, total: int, operation: str):
        """Registra o progresso de uma operação."""
        percentage = (current / total) * 100 if total > 0 else 0
        self.logger.info(f"PROGRESS: {operation} - {current}/{total} ({percentage:.2f}%)", extra={
            "progress_operation": operation,
            "progress_current": current,
            "progress_total": total,
            "progress_percentage": percentage
        })

# Instância global do logger
logger = ETLLogger()