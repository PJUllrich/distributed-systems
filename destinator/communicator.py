import logging
import threading
from queue import Queue

from destinator.connector import Connector
from destinator.message_handler import MessageHandler

logger = logging.getLogger(__name__)


class Communicator(threading.Thread):
    def __init__(self, device):
        super().__init__()
        self.daemon = True
        self.cancelled = False

        self.device = device
        self.queue_receive = Queue()
        self.queue_deliver = Queue()
        self.queue_send = Queue()

        self.message_handler = MessageHandler(self.device.category, self)
        self.connector = Connector(self.device.category, self.queue_receive)

    def run(self):
        """
        Runs the VectorTimestamp Thread.

        This also starts the Receiver Thread, which listens to incoming messages.
        Starts pulling messages or commands from the Queues shared with the Receiver
        Thread and the Device Thread.
        """
        self.connector.start()
        self.message_handler.start_discover()
        self.pull()

    def pull(self):
        """
        Pulls any command from the Queue shared with the Device Thread.
        Executes the command, if there is any.

        Pulls messages from the Queue shared with the Receiver Thread.
        Forwards the message to the handle_message function.
        """
        while not self.cancelled:
            if not self.queue_send.empty():
                msg = self.queue_send.get()
                self.connector.broadcast(msg)
                self.queue_send.task_done()

            if not self.queue_receive.empty():
                msg = self.queue_receive.get()
                self.receive(msg)
                self.queue_receive.task_done()

    def receive(self, msg):
        """
        Forwards a received message to the MessageHandler

        Parameters
        ----------
        msg:    str
            The incoming message in JSON format
        """
        self.message_handler.handle(msg)

    def send(self, text):
        """
        Forwards a text that should be sent to the MessageHandler, which then handles
        the actual sending.

        Parameters
        ----------
        msg:    str
            The message as a string (only text w/o Vector data).
        """
        self.message_handler.send(text)

    def broadcast(self, msg):
        """
        Puts a message in the Send Queue from where it will be sent to the Connector
        for broadcasting.

        Parameters
        ----------
        msg:    str
            JSON data with Vector data + Text
        """
        self.queue_send.put(msg)

    def deliver(self, msg):
        """
        Puts a message into the Queue shared with the Device Thread.

        Parameters
        ----------
        msg:    str
            The message in JSON format that should be delivered to the Device Thread
        """
        self.queue_deliver.put(msg)
