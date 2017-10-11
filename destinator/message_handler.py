import logging
import threading
from queue import Queue

from apscheduler.schedulers.background import BackgroundScheduler

import destinator.util.decorators as deco
from destinator.factories.message_factory import MessageFactory
from destinator.handlers.discovery import Discovery
from destinator.handlers.vector_timestamp import VectorTimestamp
from destinator.util.package import JsonPackage, UnpackedPackage
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
        self.history_send = []

        self.scheduler = BackgroundScheduler()
        self.scheduler.start()

        self._leader = False
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

    def send(self, message_type, payload, process=None, increment=False):
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

        package_send = MessageFactory.pack(self.vector, message_type, payload)
        package_save = UnpackedPackage(self.vector, message_type, payload)

        self.queue_send.put((process, package_send))
        self.add_to_history(package_save)

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

        # Start Phase King algorithm
        self.set_leader(self.is_leader)

    def set_leader(self, is_leader):
        logger.info(f"Am I a leader? {is_leader}")
        self._leader = is_leader

        if self.is_discovering is False:
            # Start the Phase King algorithm
            if self._leader:
                self.active_handler.handler_phase_king.start()
            else:
                self.active_handler.handler_phase_king.stop()

    @property
    def is_leader(self):
        return self._leader

    def get_old_message(self, msg_id):
        """
        Retrieves an old package from the send history with a certain message id.
        The message id equals the count of messages with the own process id.

        Parameters
        ----------
        msg_id: int
            The id of the message that should be retrieved

        Returns
        -------
        UnpackedPackage or None
        """

        own_id = self.vector.process_id
        backlog = [p for p in self.history_send if p.vector.index.get(own_id) == msg_id]
        if len(backlog) == 0:
            return None

        return backlog[-1]

    def add_to_history(self, package):
        """
        Appends a package to the history of packages sent.
        If the history becomes too big, the history array is halved.

        Parameters
        ----------
        package: UnpackedPackage
        """
        if len(self.history_send) > 10000:
            self.history_send = self.history_send[5000:]

        self.history_send.append(package)
