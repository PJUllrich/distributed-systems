import logging
import threading

from destinator.util.package import ReceivedPackage

MESSAGE_SIZE = 1024

logger = logging.getLogger(__name__)


class Listener(threading.Thread):
    def __init__(self, sock, queue):
        super().__init__()
        self.daemon = True
        self.cancelled = False

        self.sock = sock
        self.queue = queue

    def run(self):
        """
        Starts the receiving loop, which receives packages from the socket.
        """
        self.receive()

    def receive(self):
        logger.debug(f"Thread {threading.get_ident()}: "
                     f"Socket {self.sock}: Listener is now receiving.")
        while not self.cancelled:
            message, address = self.sock.recvfrom(MESSAGE_SIZE)
            logger.debug(f"Thread {threading.get_ident()}: Received from {address} "
                     f"{message}")
            package = ReceivedPackage(message, address)
            self.queue.put(package)
