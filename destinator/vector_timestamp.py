import logging
from queue import Queue

import destinator.const.messages as messages
import destinator.util.decorators as deco
from destinator.discovery import Discovery
from destinator.message_factory import MessageFactory
from destinator.util.vector import Vector

logger = logging.getLogger(__name__)


class VectorTimestamp:
    def __init__(self, connector):
        self.connector = connector
        self.discovery = Discovery(self)

        self.queue_hold_back = Queue()
        self.vector = None

    def initialize(self):
        self.vector = self.create_vector()
        self.discovery.start_discovery()

    def co_multicast(self, text):
        """
        Sends out a text together with the Vector object of the VectorTimestamp object.
        Before sending, the message counter for the Process sending is incremented by 1
        unless the Process is discovering.

        Messages are sent through the Connector class. Look at Connector.send() for
        more specifications of where the message is sent.

        Parameters
        ----------
        text:   str
            The text to be sent
        """
        if not self.discovery.discovering:
            self.vector.index[self.vector.process_id] += 1

        msg = MessageFactory.pack(self.vector, text)
        self.connector.send(msg)

    @deco.verify_message
    def b_deliver(self, msg):
        logger.debug(f"Thread {self.connector.id_process}: "
                     f"VectorTimestamp received message: {msg}")
        if self.discovery.discovering:
            self.discovery.handle_discovery(msg)

        self.handle_message(msg)

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
        id_group_own = self.connector.id_group
        id_process_own = self.connector.id_process
        id_message_own = 0

        index = {
            id_process_own: id_message_own
        }

        return Vector(id_group_own, id_process_own, index)

    def handle_message(self, msg):
        """
        Handles an incoming message.

        Parameters
        ----------
        msg:    str
            Received JSON data
        """
        vector, text = MessageFactory.unpack(msg)

        if text == messages.DISCOVERY:
            self.discovery.respond_discovery()
