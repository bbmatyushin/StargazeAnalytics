import logging


def get_logger():
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)

logger = logging.Logger
