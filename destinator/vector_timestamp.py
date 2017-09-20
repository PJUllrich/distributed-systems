import logging
import threading
from asyncio import Queue

from destinator.receiver import Receiver
from destinator.socket_factory import SocketFactory

# Time after which requests time out (in milliseconds)
REQUEST_TIMEOUT = 1000

logger = logging.getLogger(__name__)


class VectorTimestamp(threading.Thread):
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

        self.receiver = Receiver(self.sock, self.queue_receive)

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
        self.receiver.start()
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
        self.deliver(msg)
        logger.info(f"{threading.get_ident()}: Message received! {msg}")

    def send(self, msg):
        """
        Sends a message via the Multicast socket.

        Parameters
        ----------
        msg:    str
            The message to be sent

        """
        self.device.sock.sendto(msg.encode(), (self.device.category.MCAST_ADDR,
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
