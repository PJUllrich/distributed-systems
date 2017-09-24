from abc import ABC


class BaseHandler(ABC):
    def __init__(self, vector, send, deliver, next):
        self.vector = vector
        self.send = send
        self.deliver = deliver
        self.next = next
