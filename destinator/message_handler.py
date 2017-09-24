import logging

import destinator.util.decorators as deco
from destinator.handlers.discovery import Discovery
from destinator.handlers.vector_timestamp import VectorTimestamp
from destinator.util.vector import Vector

logger = logging.getLogger(__name__)


class MessageHandler:
    def __init__(self, connector):
        self.connector = connector

        self.handler = iter([Discovery, VectorTimestamp])
        self.active_handler = None
        self.vector = None

    def discover(self):
        self.vector = self.create_vector()
        self.next(self.vector)

        self.active_handler.start_discovery()

    @deco.verify_message
    def handle(self, msg):
        self.active_handler.handle(msg)

    def send(self, msg):
        self.connector.send(msg)

    def deliver(self, msg):
        self.connector.deliver(msg)

    def next(self, vector):
        handler_new = self.handler.__next__()

        self.active_handler = handler_new(vector, self.send, self.deliver, self.next)

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
