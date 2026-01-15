import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from . import settings

def setup_logger(name: str = None, log_level: int = logging.INFO) -> logging.Logger:
    """
    Sets up the root logger with both console (StreamHandler) and file (RotatingFileHandler) output.
    """
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # Prevent adding handlers multiple times if logger is already set up
    if logger.hasHandlers():
        return logger

    # Formatters
    console_format = logging.Formatter("%(message)s")  # Keep console output clean/minimal
    file_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # 1. Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # 2. File Handler
    log_dir = settings.BASE_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "app.log"

    file_handler = RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"  # 5 MB
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)

    return logger
