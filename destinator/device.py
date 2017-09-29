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

        logger.warning(f"{threading.get_ident()} - Device crashed)")

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
        if self.communicator.is_discovering is False:
            if rd.random() < 0.000001:
                #self.send(rd.randint(-10, 30))
                self.communicator.message_handler.active_handler.handler_bully\
                    .call_for_election()
        else:
            logger.info(f"{threading.get_ident()} - Device is not ready to send "
                        f"information [discovery mode]")

    def handle_message(self, msg):
        logger.info(f"{threading.get_ident()} - Device received a message: {msg}")

    def send(self, msg):
        self.communicator.send(self.category.NAME, msg)

    def set_leader(self, is_leader):
        self.communicator.set_leader(is_leader)
