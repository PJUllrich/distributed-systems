import logging
import threading

from destinator.vector_timestamp import VectorTimestamp

logger = logging.getLogger(__name__)


class Device(threading.Thread):
    """Base class for Nodes/Processes"""

    sock = None

    def __init__(self, category):
        super().__init__()
        self.deamon = True
        self.cancelled = False

        self.category = category
        self.order = VectorTimestamp(self)

    def run(self):
        self.connect()

    def connect(self):
        self.order.start()
