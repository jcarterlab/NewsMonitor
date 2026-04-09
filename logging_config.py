"""
Logging configuration module.

This module sets up application-wide logging, including console and file
handlers, formatting and log levels for the pipeline.
"""

import logging
from logging.handlers import RotatingFileHandler

def setup_logging(console_level, file_level, config):
    """
    Set up console and file logging for the application.

    Args:
        console_level (int):
            Logging level for console output.
        file_level (int):
            Logging level for file output.
        config (module):
            Configuration module containing 'LOG_DIR'.
    """
    log_file = config.LOG_DIR / 'newsmonitor.log'

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=1_000_000,
        backupCount=3,
        encoding='utf-8'
    )
    file_handler.setLevel(file_level)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    root_logger.handlers.clear()
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)