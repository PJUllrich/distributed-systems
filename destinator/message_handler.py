import logging
import threading
from queue import Queue

import destinator.util.decorators as deco
from destinator.factories.message_factory import MessageFactory
from destinator.handlers.discovery import Discovery
from destinator.handlers.vector_timestamp import VectorTimestamp
from destinator.util.vector import Vector
import destinator.const.messages as messages

logger = logging.getLogger(__name__)


class MessageHandler(threading.Thread):
    def __init__(self, communicator, connector):
        super().__init__()
        self.cancelled = False

        self.communicator = communicator
        self.connector = connector

        self.queue_send = Queue()

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
        self.vector = self.create_vector()

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
            msg = self.connector.queue_receive.get()
            self.handle(msg)

    def _transmit(self):
        """
        Pulls a message from the sending Queue of the MessageHandler class if available
        and puts the message in the sending Queue of Connector for broadcasting.
        """
        if not self.queue_send.empty():
            msg = self.queue_send.get()
            self.connector.queue_send.put(msg)

    @deco.verify_message
    def handle(self, msg):
        """
        Forwards a message to the handle function of the active handler.
        Parameters
        ----------
        msg:    str
            The incoming message in JSON format
        """
        self.active_handler.handle(msg)

    def send(self, payload, message_type=messages.UNDEFINED, process=None,
             increment=True):
        """
        Puts a message into the sending Queue. Increments the message counter by 1
        if not otherwise specified (counter should not be incremented during discovery).

        Parameters
        ----------
        text:   str
            The text to send in a message
        increment: bool
            Whether to increment the message counter of the Process or not.
        """
        if process is None:
            process = self.communicator.category.MCAST_PORT
        if increment:
            self.vector.index[self.vector.process_id] += 1

        msg = MessageFactory.pack(self.vector, payload, message_type)
        self.queue_send.put((process, msg))

    def deliver(self, msg):
        """
        Wrapper function for the deliver function of the Communicator class

        Parameters
        ----------
        msg:    str
            The message to be delivered in JSON format
        """
        self.communicator.deliver(msg)

    def end_discovery(self):
        """
        Ends the discovery procedure by setting the active handler to VectorTimestamp.
        Thus, from here on, incoming messages will be handled by the VectorTimestamp
        algorithm.
        """
        self.active_handler = VectorTimestamp(self)

    def create_vector(self):
        """
        Creates a new Vector object with the group id and process id of the Connector
        object. Sets the counter for own messages to 0.

        Returns
        -------
        Vector
            A new Vector object holding information about Group ID, Process ID,
            and own message count

        """
        id_group_own = self.communicator.category.MCAST_ADDR
        id_process_id_own = self.communicator.connector.port
        id_message_own = 0

        index = {
            id_process_id_own: id_message_own
        }

        return Vector(id_group_own, id_process_id_own, index)
