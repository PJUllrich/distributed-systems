import threading
import logging

logger = logging.getLogger(__name__)

class VectorTimestamp(threading.Thread):
    def __init__(self, device):
        super().__init__()
        self.deamon = True
        self.cancelled = False

        self.device = device

        self.queue_receive = {}
        self.queue_deliver = {}

    def run(self):
        self.receive()

    def receive(self):
        while not self.cancelled:
            message = self.device.sock.recv(255)
            logger.info("Received message")
            t = threading.Thread(target=self.handle_message, args=(message,))
            t.start()

    def handle_message(self, msg):
        pass

    def deliver(self, msg):
        pass
