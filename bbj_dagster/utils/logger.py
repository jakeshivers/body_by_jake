# bbj_dagster/utils/logger.py

import logging
import sys
import functools

def get_logger(name: str = "bbj") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

def with_logger(logger_name=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            name = logger_name or func.__name__
            logger = get_logger(name)
            logger.info(f"Running `{func.__name__}`")
            try:
                result = func(*args, **kwargs)
                logger.info(f"`{func.__name__}` completed successfully")
                return result
            except Exception as e:
                logger.exception(f"Error in `{func.__name__}`: {e}")
                raise
        return wrapper
    return decorator
