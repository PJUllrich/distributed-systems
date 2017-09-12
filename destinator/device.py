import socket
import struct

class Device:
    """Base class for Nodes"""

    MCAST_GRP = '224.1.1.1'
    MCAST_PORT = 5000

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # bind address can be limited to group
        self.sock.bind(('0.0.0.0', self.MCAST_PORT))
        mreq = struct.pack("4sl", socket.inet_aton(self.MCAST_GRP), socket.INADDR_ANY)

        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        self.connect()

    def connect(self):
        pass

    def broadcast(self):
        self.sock.sendto(b"hello", (self.MCAST_GRP, self.MCAST_PORT))

    def receive(self, msg):
        print(self.sock.recv(10240))

    def deliver(self, msg):
        pass

if __name__ == "__main__":
    device = Device()
    device2= Device()
    while True:
        device.broadcast()
        device2.receive(" ")