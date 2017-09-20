import logging
import threading

from destinator.socket_factory import SocketFactory
from destinator.vector_timestamp import VectorTimestamp

logger = logging.getLogger(__name__)


class Device(threading.Thread):
    """Base class for Nodes/Processes"""

    sock = None

    def __init__(self, category):
        super().__init__()
        self.deamon = True
        self.cancelled = False

        self.category = category
        self.order = VectorTimestamp(self)

    def run(self):
        self.connect()
        self.listen()

    def connect(self):
        self.sock = SocketFactory.create_socket(self.category.MCAST_ADDR,
                                                self.category.MCAST_PORT)
        logger.info(f"Thread {threading.get_ident()} is connected to Multicast Socket")

    def listen(self):
        self.order.start()

    def broadcast(self, msg):
        self.sock.sendto(msg.encode(), (self.category.MCAST_ADDR,
                                        self.category.MCAST_PORT))
