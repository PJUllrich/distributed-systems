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
        """
        Runs the Device Thread.
        This also starts the Thread of the VectorTimestamp associated with the Device
        object. The Device Thread also starts pulling messages from the Queue shared
        with the VectorTimestamp Thread
        """
        self.order.start()
        self.pull()

    def pull(self):
        """
        Pulls messages from the Queue shared with the VectorTimestamp Thread.
        Forwards a message to the handle_message function if there is any message.
        """
        while not self.cancelled:
            if not self.order.queue_deliver.empty():
                msg = self.order.queue_deliver.get()
                self.handle_message(msg)
                self.order.queue_deliver.task_done()

    def handle_message(self, msg):
        pass
