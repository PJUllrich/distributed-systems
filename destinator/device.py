import logging
from threading import Thread

from destinator.socket_factory import SocketFactory

logger = logging.getLogger(__name__)


class Device:
    """Base class for Nodes"""

    sock = None

    def __init__(self, category):
        self.category = category

        self.connect()

    def connect(self):
        self.sock = SocketFactory.create_socket(self.category.MCAST_ADDR,
                                                self.category.MCAST_PORT)

        self.listen()

    def listen(self):
        t = Thread(target=self.receive)
        t.start()

        logger.info("Device is connected")

    def broadcast(self, msg):
        self.sock.sendto(msg.encode(), (self.category.MCAST_ADDR,
                                        self.category.MCAST_PORT))

    def receive(self):
        while True:
            message = self.sock.recv(255)
            t = Thread(target=self.handle_message, args=(message,))
            t.start()

    def handle_message(self, msg):
        print(msg)

    def deliver(self, msg):
        pass
