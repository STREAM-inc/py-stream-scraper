import logging


def setup_logger(level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger("py_stream_scraper")
    if not logger.handlers:
        h = logging.StreamHandler()
        fmt = logging.Formatter(
            "[%(asctime)s] %(levelname)s %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        h.setFormatter(fmt)
        logger.addHandler(h)
    logger.setLevel(level)
    return logger
