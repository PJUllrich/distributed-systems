import logging
import threading
import time

import destinator.const.messages as messages
from destinator.factories.message_factory import MessageFactory
from destinator.handlers.base_handler import BaseHandler

logger = logging.getLogger(__name__)

# Time (in seconds) a process will wait until stopping to discover new Processes
DISCOVERY_TIMEOUT = 5


class Discovery(BaseHandler):
    def __init__(self, parent_handler):
        super().__init__(parent_handler)

        self.discovery_start = None

        self.handlers = {
            messages.DISCOVERY: self.discovery,
            messages.DISCOVERY_RESPONSE: self.discovery_response
        }

    def start_discovery(self):
        """
        Creates a new Vector information containing information about the
        VectorTimestamp object.

        Sends out a DISCOVERY message in order to discover other active processes in the
        multicast group.
        """

        self.parent.send(messages.DISCOVERY, increment=False)
        self.discovery_start = time.time()

    def handle(self, msg):
        """
        Checks whether the DISCOVERY_TIMEOUT is reached. If yes, changes the active
        handler in MessageHandler and passes the message back to MessageHandler.

        If not timed out, unpacks the vector and text from a message and passes these
        parameters on to the preset handler for the message text.

        Parameters
        ----------
        msg:    str
            Received JSON data
        """
        if self.timeout():
            self.parent.end_discover()
            self.parent.handle(msg)
            return

        vector, text = MessageFactory.unpack(msg)
        handle_function = self.handlers.get(text, self.default)
        handle_function(vector, text)

    def discovery(self, vector, text):
        """
        Adds a Process ID to the Vector index if the index does not yet contain the
        Process ID.

        Sends a response to a DISCOVERY message containing identifying information
        about the VectorTimestamp object.
        """
        if vector.process_id not in self.parent.vector.index:
            self.parent.vector.index[vector.process_id] = \
                vector.index.get(vector.process_id)

            logger.info(f"Thread {threading.get_ident()}: "
                        f"Leader added Process: {vector.process_id}."
                        f"New index: {self.parent.vector.index}")

        self.parent.send(messages.DISCOVERY_RESPONSE, increment=False)

    def discovery_response(self, vector, text):
        """
        Handles a DISCOVERY_RESPONSE message. Adds any Process IDs to the own Vector
        index and updates the message counts of the existing Process IDs.

        Parameters
        ----------
        vector: Vector
            The Vector object received with the message
        text:   str
            The message text, should be 'DISCOVERY_RESPONSE'
        """
        self.parent.vector.index.update(vector.index)
        logger.info(f"Thread {threading.get_ident()}: "
                    f"Process received DISCOVERY_RESPONSE and added Process: "
                    f"{vector.process_id}. New index: {self.parent.vector.index}")

    def default(self, vector, text):
        """
        The default function to handle incoming messages. At the moment, only logs the
        reception of the message.

        Parameters
        ----------
        vector: Vector
            The Vector object received with the message
        text:   str
            The message text received with the message
        """
        # TODO: Figure out what the default function in Discovery mode should do.
        logger.debug(f"MessageHandler in Discovery mode received a message other than "
                     f"DISCOVERY or DISCOVERY_RESPONSE")

    def timeout(self):
        """
        Checks whether DISCOVERY_TIMEOUT is reached.

        Returns
        -------
        bool
            Whether Discovery timed out or not
        """
        if time.time() - self.discovery_start >= DISCOVERY_TIMEOUT:
            logger.debug(f"Thread {threading.get_ident()}: "
                         f"Discovery Mode timed out.")
            return True

        return False
