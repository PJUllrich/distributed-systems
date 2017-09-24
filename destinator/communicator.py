import logging
import threading
from queue import Queue

from destinator.factories.message_factory import MessageFactory
from destinator.factories.socket_factory import SocketFactory
from destinator.message_handler import Communicator
from destinator.util.listener import Listener

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

        self.message_handler = Communicator(self)
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
        self.message_handler.handle(msg)

    def send(self, text, increment=True):
        """
        Sends a message via the Multicast socket.

        Parameters
        ----------
        text:    str
            The text to be sent
        increment:  bool
            Whether to increment the message counter in the Vector or not
        """
        if increment:
            self.vector.index[self.vector.process_id] += 1

        msg = MessageFactory.pack(self.vector, text)

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
