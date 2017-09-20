import logging
import threading
import uuid

import sys


def setup_logger(filename):
    """
    Sets up a simple logger which logs to a 'output.log' file.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(filename)
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)

    mac_addr = hex(uuid.getnode()).replace('0x', '')
    thread_id = str(threading.get_ident())
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - '+mac_addr+'-'+thread_id+' - %(name)s: %('
                                                             'message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    logger.info('Logger is created.')