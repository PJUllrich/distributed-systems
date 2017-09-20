import threading
from asyncio import Queue

from destinator.receiver import Receiver
from destinator.socket_factory import SocketFactory

# Time after which requests time out (in milliseconds)
REQUEST_TIMEOUT = 1000


class VectorTimestamp(threading.Thread):
    sock = None

    def __init__(self, device):
        super().__init__()
        self.deamon = True
        self.cancelled = False

        self.device = device
        self.queue_receive = Queue()
        self.queue_deliver = Queue()
        self.queue_execute = Queue()

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
            if not self.queue_execute.empty():
                cmd, args = self.queue_execute.get()
                cmd(*args)
                self.queue_execute.task_done()

            if not self.queue_receive.empty():
                msg = self.queue_receive.get()
                self.handle_message(msg)
                self.queue_receive.task_done()

    def handle_message(self, msg):
        pass

    def deliver(self, msg):
        pass

    def broadcast(self, msg):
        """
        Broadcasts a message via the Multicast socket.

        Parameters
        ----------
        msg:    str
            The message to be send

        """
        self.device.sock.sendto(msg.encode(), (self.device.category.MCAST_ADDR,
                                               self.device.category.MCAST_PORT))
