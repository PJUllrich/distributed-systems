import logging
from queue import Queue

from destinator.connector import Connector
from destinator.message_handler import MessageHandler

logger = logging.getLogger(__name__)


class Communicator:
    def __init__(self, device):
        super().__init__()
        self.cancelled = False

        self.device = device
        self.queue_deliver = Queue()

        self.connector = Connector(self)
        self.message_handler = MessageHandler(self, self.connector)

    @property
    def category(self):
        return self.device.category

    def set_leader(self, is_leader):
        self.message_handler.leader = is_leader
        self.connector.port = self.category.STARTING_PORT

    @property
    def is_discovering(self):
        return self.message_handler.is_discovering

    def start(self):
        """
        Starts the Connector thread, which starts listening for packages on a socket.
        Starts the MessageHandler thread, which starts the discovery procedure,
        handles incoming messages and delivers them back to the Communicator.
        """
        self.connector.start()
        self.message_handler.start()

    def send(self, message_type, payload):
        """
        Forwards a text that should be sent to the MessageHandler, which then handles
        the actual sending.

        Parameters
        ----------
        payload:  str
            The message as a string (only text w/o Vector data).
        message_type: str
            The group of the message
        """
        self.message_handler.send(message_type, payload, increment=True)

    def deliver(self, msg):
        """
        Puts a message into the Queue shared with the Device Thread.

        Parameters
        ----------
        msg:    str
            The message in JSON format that should be delivered to the Device Thread
        """
        self.queue_deliver.put(msg)
