from abc import ABC


class BaseHandler(ABC):
    def __init__(self, message_handler):
        self.parent = message_handler
