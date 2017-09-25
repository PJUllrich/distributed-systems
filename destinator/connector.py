from destinator.factories.socket_factory import SocketFactory
from destinator.util.listener import Listener


class Connector:
    def __init__(self, category, queue):
        self.queue = queue
        self.category = category
        self.sock = None

        self.connect()
        self.listener = Listener(self.sock, self.queue)

    def connect(self):
        """
        Connects to a Multicast socket on the address and port specified in the Category
        of the Device
        """
        self.sock = SocketFactory.create_socket(self.category.MCAST_ADDR,
                                                self.category.MCAST_PORT)

    def start(self):
        """
        Starts the Listener in a new Thread.
        """
        self.listener.start()

    def broadcast(self, msg):
        """
        Broadcasts a message on the multicast socket.

        Parameters
        ----------
        msg:    str
            The message (Vector + text) in JSON format
        """
        self.sock.sendto(msg.encode(), (self.category.MCAST_ADDR,
                                        self.category.MCAST_PORT))
