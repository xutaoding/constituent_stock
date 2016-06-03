import logging

from config import *


def get_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter('[%(asctime)s %(levelname)s]: %(message)s'))

    logger.addHandler(console)
    return logger

logger = get_logger()
