import logging
import threading

import destinator.util.decorators as deco
from destinator.factories.message_factory import MessageFactory
from destinator.handlers.discovery import Discovery
from destinator.handlers.vector_timestamp import VectorTimestamp
from destinator.util.vector import Vector

logger = logging.getLogger(__name__)


class MessageHandler:
    def __init__(self, category, communicator):
        self.communicator = communicator
        self.category = category

        self.active_handler = None

        self.vector = None

    def start_discover(self):
        """
        Creates a new Vector. Sets the new active handler to Discovery and starts the
        discovery process.
        """
        self.vector = self.create_vector()

        self.active_handler = Discovery(self)
        self.active_handler.start_discovery()

    def end_discover(self):
        """
        Ends the discovery procedure by setting the active handler to VectorTimestamp.
        Thus, from here on, incoming messages will be handled by the VectorTimestamp
        algorithm.
        """
        self.active_handler = VectorTimestamp(self)

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

    def send(self, text, increment=True):
        """
        Forwards a message to the Communicator class. Increments the message counter by 1
        if not otherwise specified (counter should not be incremented during discovery).

        Parameters
        ----------
        text:   str
            The text to send in a message
        increment: bool
            Whether to increment the message counter of the Process or not.
        """
        if increment:
            self.vector.index[self.vector.process_id] += 1

        msg = MessageFactory.pack(self.vector, text)
        self.communicator.broadcast(msg)

    def deliver(self, msg):
        """
        Wrapper function for the deliver function of the Communicator class

        Parameters
        ----------
        msg:    str
            The message to be delivered in JSON format
        """
        self.communicator.deliver(msg)

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
        id_group_own = self.category.MCAST_ADDR
        id_process_own = threading.get_ident()
        id_message_own = 0

        index = {
            id_process_own: id_message_own
        }

        return Vector(id_group_own, id_process_own, index)
