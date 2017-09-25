import logging
from queue import Queue

import destinator.util.decorators as deco
from destinator.factories.message_factory import MessageFactory
from destinator.handlers.base_handler import BaseHandler

logger = logging.getLogger(__name__)


class VectorTimestamp(BaseHandler):
    def __init__(self, parent_handler):
        super().__init__(parent_handler)

        self.queue_hold_back = Queue()

    def handle(self, msg):
        """
        Handles an incoming message.

        Parameters
        ----------
        msg:    str
            Received JSON data
        """

        vector, text = MessageFactory.unpack(msg)
        logger.info(f"VectorTimestamp received message: {text} from {vector}")

        # TODO: ADD THE ACTUAL ALGORITHM

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

        self.parent.send(text)

    @deco.verify_message
    def b_deliver(self, msg):
        pass

    def co_deliver(self, msg):
        """
        Delivers a message to the Connector's Queue shared with a Device object

        Parameters
        ----------
        msg:    str
            A String containing the message (Identifier + text) in JSON format

        """
        self.parent.deliver(msg)
