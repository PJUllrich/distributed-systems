import threading

# Time after which requests time out (in milliseconds)
REQUEST_TIMEOUT = 1000

MESSAGE_SIZE = 255


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
            message = self.device.sock.recv(MESSAGE_SIZE)
            t = threading.Thread(target=self.handle_message, args=(message,))
            t.start()

    def handle_message(self, msg):
        pass

    def deliver(self, msg):
        pass

    def broadcast(self, msg):
        self.device.sock.sendto(msg.encode(), (self.device.category.MCAST_ADDR,
                                               self.device.category.MCAST_PORT))
