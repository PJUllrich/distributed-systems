import socket
import struct
from threading import Thread


class Device:
    """Base class for Nodes"""

    socket = None

    def __init__(self, category):
        self.category = category

        self.connect()

        self.listen()

    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.socket.bind(('0.0.0.0', self.category.MCAST_PORT))

        membership = struct.pack("4sl",
                                 socket.inet_aton(self.category.MCAST_ADDR),
                                 socket.INADDR_ANY)
        self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)

    def listen(self):
        t = Thread(target=self.receive)
        t.start()

    def broadcast(self, msg):
        print("Sending message")
        self.socket.sendto(msg.encode(), (self.category.MCAST_ADDR,
                                          self.category.MCAST_PORT))

    def receive(self):
        while True:
            message = self.socket.recv(255)
            print(message)

    def deliver(self, msg):
        pass
