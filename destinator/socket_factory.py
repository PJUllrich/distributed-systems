import socket
import struct
import sys


class SocketFactory:
    @staticmethod
    def create_socket(addr, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, SocketFactory.reuse_option(), 1)
        sock.bind(('', port))

        membership = struct.pack("4sl", socket.inet_aton(addr), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)

        return sock

    @staticmethod
    def reuse_option():
        platform = sys.platform

        if platform == 'darwin':
            return socket.SO_REUSEPORT
        else:
            return socket.SO_REUSEADDR
