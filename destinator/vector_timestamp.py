import threading
from asyncio import Queue

from destinator.receiver import Receiver

# Time after which requests time out (in milliseconds)
REQUEST_TIMEOUT = 1000


class VectorTimestamp(threading.Thread):
    def __init__(self, device):
        super().__init__()
        self.deamon = True
        self.cancelled = False

        self.device = device
        self.receiver = Receiver(self.device.sock, self.queue_receive)

        self.queue_receive = Queue()
        self.queue_deliver = Queue()
        self.queue_execute = Queue()

    def run(self):
        self.receiver.start()
        self.pull()

    def pull(self):
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
        self.device.sock.sendto(msg.encode(), (self.device.category.MCAST_ADDR,
                                               self.device.category.MCAST_PORT))
