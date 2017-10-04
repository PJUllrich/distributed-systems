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
        self.port = -1  # Also acts as the process_id

        self.queue_receive = Queue()
        self.queue_send = Queue()
        self.sock_multicast = None
        self.sock_individual = None

        self._connect()
        self.listener_multicast = Listener(self.sock_multicast, self.queue_receive)
        self.listener_individual = None

    def _connect(self):
        """
        Connects to a Multicast socket on the address and port specified in the Category
        of the Device
        """
        self.sock_multicast = self._create_socket(self.communicator.category.MCAST_PORT)

    def start_individual_listener(self, port):
        if self.listener_individual is not None:
            self.listener_individual.cancelled = True

        self.port = port
        self.sock_individual = self._create_socket(self.port)
        self.listener_individual = Listener(self.sock_individual, self.queue_receive)
        self.listener_individual.start()

    def _create_socket(self, port):
        return SocketFactory.create_socket(self.communicator.category.MCAST_ADDR, port)

    def run(self):
        """
        Starts the Listener in a new Thread.
        Starts sending out messages in the Sending Queue.
        """
        self.listener_multicast.start()

        while not self.cancelled:
            self.send()

    def send(self):
        """
        Sends out all messages in the Sending Queue.
        """
        while not self.queue_send.empty():
            process_id, msg = self.queue_send.get()
            self._send(msg, process_id)

    def _send(self, msg, process_id):
        """
        Send a message to an individual device
        Parameters
        ----------
        msg: str
            The message to send
        process_id
            The process_id of the receiving device
        """
        logger.debug(f"Thread {threading.get_ident()}: Sending from {self.port} to "
                     f"{process_id}: {msg}")
        self.sock_multicast.sendto(msg.encode(), (self.communicator.category.MCAST_ADDR,
                                                 process_id))
