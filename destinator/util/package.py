from destinator.factories.message_factory import MessageFactory


class Package:
    def __init__(self, content):
        self.content = content


class ReceivedPackage(Package):
    def __init__(self, content, sender):
        super().__init__(content)
        self.sender = sender


class JsonPackage(ReceivedPackage):
    def __init__(self, package):
        """
        Use a ReceivedPackage to get a JsonPackage
        """
        super().__init__(package, package.sender)

        vector, message_type, payload = MessageFactory.unpack(package.content)
        self.vector = vector
        self.message_type = message_type
        self.payload = payload
