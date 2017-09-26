import logging
import threading
from queue import Queue

from destinator.factories.socket_factory import SocketFactory
from destinator.util.listener import Listener

logger = logging.getLogger(__name__)


class Connector(threading.Thread):
    def __init__(self, communicator):
        super().__init__()
        self.cancelled = False
        self.communicator = communicator

        self.queue_receive = Queue()
        self.queue_send = Queue()
        self.sock = None

        self._connect()
        self.listener = Listener(self.sock, self.queue_receive)

    def _connect(self):
        """
        Connects to a Multicast socket on the address and port specified in the Category
        of the Device
        """
        self.sock = SocketFactory.create_socket(self.communicator.category.MCAST_ADDR,
                                                self.communicator.category.MCAST_PORT)

    def run(self):
        """
        Starts the Listener in a new Thread.
        Starts sending out messages in the Sending Queue.
        """
        self.listener.start()

        while not self.cancelled:
            self.send()

    def send(self):
        """
        Sends out all messages in the Sending Queue.
        """
        while not self.queue_send.empty():
            msg = self.queue_send.get()
            self._broadcast(msg)

    def _broadcast(self, msg):
        """
        Broadcasts a message on the multicast socket.

        Parameters
        ----------
        msg:    str
            The message (Vector + text) in JSON format
        """
        self.sock.sendto(msg.encode(), (self.communicator.category.MCAST_ADDR,
                                        self.communicator.category.MCAST_PORT))
