import logging
import threading

from destinator.vector_timestamp import VectorTimestamp

logger = logging.getLogger(__name__)


class Device(threading.Thread):
    """Base class for Nodes/Processes"""

    def __init__(self, category):
        super().__init__()
        self.deamon = True
        self.cancelled = False

        self.category = category
        self.order = VectorTimestamp(self)

    def run(self):
        self.order.start()
        self.pull()

    def pull(self):
        while not self.cancelled:
            if not self.order.queue_deliver.empty():
                msg = self.order.queue_deliver.get()
                self.handle_message(msg)
                self.order.queue_deliver.task_done()

    def handle_message(self, msg):
        pass
