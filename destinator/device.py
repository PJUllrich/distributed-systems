import logging
import random as rd
import threading

from destinator.communicator import Communicator

logger = logging.getLogger(__name__)


class Device(threading.Thread):
    """Base class for Nodes/Processes"""

    def __init__(self, category):
        super().__init__()
        self.cancelled = False

        self.history = []

        self.category = category
        self.communicator = Communicator(self)

    def run(self):
        """
        Runs the Device Thread.
        This also starts the Thread of the VectorTimestamp associated with the Device
        object. The Device Thread also starts pulling messages from the Queue shared
        with the VectorTimestamp Thread
        """
        self.communicator.start()

        while not self.cancelled:
            self.pull()
            self.work()

        logger.error(f"{threading.get_ident()} - Device crashed)")

    def pull(self):
        """
        Pulls messages from the Queue shared with the VectorTimestamp Thread.
        Forwards a message to the handle_message function if there is any message.
        """
        if not self.communicator.queue_deliver.empty():
            msg = self.communicator.queue_deliver.get()
            self.handle_message(msg)
            self.communicator.queue_deliver.task_done()

    def work(self):
        if self.communicator.is_discovering:
            logger.debug(f"{threading.get_ident()} - Device is not ready to send "
                         f"information [discovery mode]")
            return

        if rd.random() < 0.0001:
            self.send(rd.randint(1, 30))

    def handle_message(self, msg):
        self.history.append(msg)

        if len(self.history) > 4:
            avg = sum(self.history) / len(self.history)
            logger.info(f"{threading.get_ident()} - Average of {len(self.history)} "
                        f"messages: {avg:.2f}")
            self.history.clear()

    def send(self, msg):
        self.communicator.send(self.category.NAME, msg)

    def set_leader(self, is_leader):
        self.communicator.set_leader(is_leader)
