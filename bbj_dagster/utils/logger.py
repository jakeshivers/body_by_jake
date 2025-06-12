# utils/logger.py

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
    def decorator(inner_fn):  # 👈 Avoid names like func or wrap_func
        @functools.wraps(inner_fn)
        def logged_fn(*args, **kwargs):  # 👈 Avoid any names that look like DAG assets
            logger = get_logger(logger_name or inner_fn.__name__)
            logger.info(f"Running `{inner_fn.__name__}`")
            try:
                result = inner_fn(*args, **kwargs)
                logger.info(f"`{inner_fn.__name__}` completed successfully")
                return result
            except Exception as e:
                logger.exception(f"Error in `{inner_fn.__name__}`: {e}")
                raise
        return logged_fn
    return decorator
