import sys
import logging
from pathlib import Path
from typing import Optional
from ..config.settings import Config
from colorlog import ColoredFormatter

def setup_logger(name: str, level: Optional[str] = None, enable_console_logging: Optional[bool] = True, enable_file_logging: Optional[bool] = True) -> logging.Logger:
    logger = logging.getLogger(name)
    logger_level = level or Config.LOG_LEVEL

    try:
        logger.setLevel(getattr(logging, logger_level.upper()))
    except AttributeError:
        logger.setLevel(logging.INFO)
        logger.warning(f"Warning: Unknown log level '{logger_level}', using INFO level.")

    logger.handlers.clear()

    if enable_console_logging:
        console_formatter = ColoredFormatter(
            fmt = "%(log_color)s[%(asctime)s] - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red",
                }
            )
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    if enable_file_logging:
        log_path = Path("logs")
        log_path.mkdir(exist_ok=True)
        log_file = log_path / f"{name}.log"

        file_formatter = logging.Formatter(
            fmt="[%(asctime)s] - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        try:
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            logger.warning(f"Cannot create log file {log_file}: {e}.")
    
    logger.propagate = False

    return logger

polygon_logger = setup_logger("polygon_producer")
kafka_logger = setup_logger("kafka_consumer")
spark_logger = setup_logger("spark_processor")
snowflake_logger = setup_logger("snowflake_connector")