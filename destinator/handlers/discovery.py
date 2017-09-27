import logging
import threading

import destinator.const.messages as messages
from destinator.factories.message_factory import MessageFactory
from destinator.handlers.base_handler import BaseHandler

logger = logging.getLogger(__name__)

# Time (in seconds) a process will wait until stopping to discover new Processes
DISCOVERY_TIMEOUT = 5


class Discovery(BaseHandler):
    def __init__(self, parent_handler):
        super().__init__(parent_handler)

    def start_discovery(self):
        """
        Creates a new Vector information containing information about the
        VectorTimestamp object.

        Sends out a DISCOVERY message in order to discover other active processes in the
        multicast group.
        """
        if self.parent.leader:
            self.end_discovery()
            return

        self.parent.send(messages.DISCOVERY, increment=False)

    def handle(self, msg):
        """
        Overwrites the handle function from the BaseHandler parent class. Calls the
        super handle function with the message. Checks afterwards whether the received
        message was a DISCOVERY_RESPONSE message. Ends discovery, if yes.

        Parameters
        ----------
        msg:    str
            The incoming message in JSON format
        """
        super().handle(msg)

        _, text = MessageFactory.unpack(msg)
        if text == messages.DISCOVERY_RESPONSE:
            self.end_discovery()

    def end_discovery(self):
        """
        Ends the discovery and calls the end_discovery function of the Root handler,
        which switches the active handler from the Discovery to the VectorTimestamp
        handler.
        """
        logger.debug(f"Thread {threading.get_ident()}: Discovery Mode ended.")
        self.parent.end_discovery()
