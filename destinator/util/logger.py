import logging
import sys
import uuid


def setup_logger(filename):
    """
    Sets up a simple logger which logs to a 'output.log' file.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(filename)
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)

    mac_addr = hex(uuid.getnode()).replace('0x', '')
    formatter = logging.Formatter(
        f'%(asctime)s - %(levelname)s - {mac_addr} - %(name)s: %(message)s')

    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    logger.info('Logger is created.')
