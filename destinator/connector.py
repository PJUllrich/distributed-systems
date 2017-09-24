import logging
import threading
from queue import Queue

from destinator.socket_factory import SocketFactory
from destinator.util.listener import Listener
from destinator.vector_timestamp import VectorTimestamp

logger = logging.getLogger(__name__)


class Connector(threading.Thread):
    sock = None

    def __init__(self, device):
        super().__init__()
        self.deamon = True
        self.cancelled = False

        self.device = device
        self.queue_receive = Queue()
        self.queue_deliver = Queue()
        self.queue_send = Queue()

        self.connect()

        self.ordering = VectorTimestamp(self)
        self.listener = Listener(self.sock, self.queue_receive)

    @property
    def id_group(self):
        return self.device.category.MCAST_ADDR

    @property
    def id_process(self):
        return threading.get_ident()

    def connect(self):
        """
        Connects to a Multicast socket on the address and port specified in the Category
        of the Device
        """
        self.sock = SocketFactory.create_socket(self.device.category.MCAST_ADDR,
                                                self.device.category.MCAST_PORT)

    def run(self):
        """
        Runs the VectorTimestamp Thread.

        This also starts the Receiver Thread, which listens to incoming messages.
        Starts pulling messages or commands from the Queues shared with the Receiver
        Thread and the Device Thread.
        """
        self.listener.start()
        self.ordering.initialize()
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
                self.send(msg)
                self.queue_send.task_done()

            if not self.queue_receive.empty():
                msg = self.queue_receive.get()
                self.receive(msg)
                self.queue_receive.task_done()

    def receive(self, msg):
        self.ordering.b_deliver(msg)

    def send(self, msg):
        """
        Sends a message via the Multicast socket.

        Parameters
        ----------
        msg:    str
            The message to be sent

        """
        self.sock.sendto(msg.encode(), (self.device.category.MCAST_ADDR,
                                        self.device.category.MCAST_PORT))

    def deliver(self, msg):
        """
        Puts a message into the Queue shared with the Device Thread.

        Parameters
        ----------
        msg:    str
            The message in JSON format that should be delivered to the Device Thread
        """
        self.queue_deliver.put(msg)
