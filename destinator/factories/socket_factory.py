import socket
import struct
import sys


class SocketFactory:
    @staticmethod
    def create_socket(addr, port):
        """
        Creates a connection to a Multicast socket with a specified port.
        Parameters
        ----------
        addr:   str
            The address of the multicast socket to which a connection should be
            established
        port:   int
            The port of the Multicast socket to which a connection should be established

        Returns
        -------
        Socket
            The established socket connection, if a connection could be created
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, SocketFactory.reuse_option(), 1)
        sock.bind(('', port))

        membership = struct.pack("4sl", socket.inet_aton(addr), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)

        return sock

    @staticmethod
    def reuse_option():
        """
        Returns a socket reuse option. When the application runs on a Mac, the reuse
        port option is returned. Otherwise, the reuse address option is returned.

        Returns
        -------
        int
            The code of the reuse option specified in the built-in socket package
        """
        platform = sys.platform

        if platform == 'darwin':
            return socket.SO_REUSEPORT
        else:
            return socket.SO_REUSEADDR
