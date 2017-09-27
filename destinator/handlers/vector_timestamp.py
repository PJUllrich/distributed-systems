import logging
from queue import Queue

from destinator.factories.message_factory import MessageFactory
from destinator.handlers.base_handler import BaseHandler

logger = logging.getLogger(__name__)


class VectorTimestamp(BaseHandler):
    def __init__(self, parent_handler):
        super().__init__(parent_handler)

        self.queue_hold_back = Queue()

    def default(self, vector, text):
        logger.info(f'VectorTimestamp called Default function for message: {text}')

        msg = MessageFactory.pack(vector, text)
        self.b_deliver(msg)

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

    def b_deliver(self, msg):
        self.queue_hold_back.put(msg)

        




    def co_deliver(self, msg):
        """
        Delivers a message to the Connector's Queue shared with a Device object

        Parameters
        ----------
        msg:    str
            A String containing the message (Identifier + text) in JSON format

        """
        self.parent.deliver(msg)
