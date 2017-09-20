import logging
import threading

from destinator.socket_factory import SocketFactory

logger = logging.getLogger(__name__)


class Device(threading.Thread):
    """Base class for Nodes"""

    sock = None

    def __init__(self, category):
        super().__init__()
        self.deamon = True
        self.cancelled = False

        self.category = category

    def run(self):
        self.connect()

    def connect(self):
        self.sock = SocketFactory.create_socket(self.category.MCAST_ADDR,
                                                self.category.MCAST_PORT)

        self.listen()

    def listen(self):
        t = threading.Thread(target=self.receive)
        t.start()

        logger.info(f"Thread {threading.get_ident()} is connected to Multicast Socket")

    def broadcast(self, msg):
        self.sock.sendto(msg.encode(), (self.category.MCAST_ADDR,
                                        self.category.MCAST_PORT))

    def receive(self):
        while True:
            message = self.sock.recv(255)
            t = threading.Thread(target=self.handle_message, args=(message,))
            t.start()

    def handle_message(self, msg):
        print(msg)

    def deliver(self, msg):
        pass
