import logging
import threading
from queue import Queue

from apscheduler.schedulers.background import BackgroundScheduler

import destinator.util.decorators as deco
from destinator.factories.message_factory import MessageFactory
from destinator.handlers.discovery import Discovery
from destinator.handlers.vector_timestamp import VectorTimestamp
from destinator.util.package import JsonPackage
from destinator.util.vector import Vector

logger = logging.getLogger(__name__)

logging.getLogger('apscheduler.scheduler').propagate = False
logging.getLogger('apscheduler.executors.default').propagate = False


class MessageHandler(threading.Thread):
    def __init__(self, communicator, connector):
        super().__init__()
        self.cancelled = False
        self.is_discovering = None

        self.communicator = communicator
        self.connector = connector

        self.queue_send = Queue()

        self.scheduler = BackgroundScheduler()
        self.scheduler.start()

        self.leader = False
        self.active_handler = None
        self.vector = None

    def run(self):
        """
        Creates a new Vector.
        Sets the new active handler to Discovery and starts the discovery process.
        Starts pulling messages from the Connector.
        Starts transmitting messages to the Connector for broadcasting.
        """
        self.vector = Vector.create_vector(
            self.communicator.category.MCAST_ADDR,
            self.communicator.connector.port
        )

        self.is_discovering = True
        self.active_handler = Discovery(self)
        self.active_handler.start_discovery()

        while not self.cancelled:
            self._pull()
            self._transmit()

    def _pull(self):
        """
        Pulls a message from the Connector receiving Queue if a message is
        available and handles the message.
        """
        if not self.connector.queue_receive.empty():
            package = self.connector.queue_receive.get()
            json_package = JsonPackage(package)
            self.handle(json_package)

    def _transmit(self):
        """
        Pulls a message from the sending Queue of the MessageHandler class if available
        and puts the message in the sending Queue of Connector for broadcasting.
        """
        if not self.queue_send.empty():
            msg = self.queue_send.get()
            self.connector.queue_send.put(msg)

    @deco.verify_message
    def handle(self, package):
        """
        Forwards a message to the handle function of the active handler.
        Parameters
        ----------
        package: Package
            The incoming package
        """
        self.active_handler.handle(package)

    def send(self, message_type, payload, process=None, increment=True):
        """
        Puts a message into the sending Queue. Increments the message counter by 1
        if not otherwise specified (counter should not be incremented during discovery).

        Parameters
        ----------
        payload: str
            The text to send in a message
        message_type: str
            The group of the message
        process: int
            The process to send the payload to (None = all processes)
        increment: bool
            Whether to increment the message counter of the Process or not.
        """
        if process is None:
            process = self.communicator.category.MCAST_PORT
        if increment:
            self.vector.index[self.vector.process_id] += 1

        msg = MessageFactory.pack(self.vector, message_type, payload)
        self.queue_send.put((process, msg))

    def deliver(self, package):
        """
        Wrapper function for the deliver function of the Communicator class

        Parameters
        ----------
        package:    JsonPackage
            The package whose content should be delivered
        """
        logger.debug(
            f"{threading.get_ident()} - Delivering message: {package.vector.index}")
        self.communicator.deliver(package.payload)

    def end_discovery(self):
        """
        Ends the discovery procedure by setting the active handler to VectorTimestamp.
        Thus, from here on, incoming messages will be handled by the VectorTimestamp
        algorithm.
        """
        logger.info(f"Thread {threading.get_ident()}: Discovery Mode ended.")
        self.active_handler = VectorTimestamp(self)
        self.is_discovering = False
