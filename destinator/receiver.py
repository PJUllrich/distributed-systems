import threading

MESSAGE_SIZE = 255


class Receiver(threading.Thread):
    def __init__(self, sock, queue):
        super().__init__()
        self.deamon = True
        self.cancelled = False

        self.sock = sock
        self.queue = queue

    def run(self):
        self.receive()

    def receive(self):
        while not self.cancelled:
            message = self.sock.recv(MESSAGE_SIZE)
            self.queue.put(message)
