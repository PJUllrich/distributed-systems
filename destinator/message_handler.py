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

        self.handler = [Discovery, VectorTimestamp]
        self.active_handler = None

        self.vector = None

    def start_discover(self):
        self.vector = self.create_vector()

        self.set_handler(0)
        self.active_handler.start_discovery()

    @deco.verify_message
    def handle(self, msg):
        self.active_handler.handle(msg)

    def send(self, text, increment=True):
        if increment:
            self.vector.index[self.vector.process_id] += 1

        msg = MessageFactory.pack(self.vector, text)
        self.communicator.broadcast(msg)

    def deliver(self, msg):
        self.communicator.deliver(msg)

    def end_discover(self):
        self.set_handler(1)

    def set_handler(self, idx):
        handler_new = self.handler[idx]
        self.active_handler = handler_new(self)

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
