class Package:
    def __init__(self, content):
        self.content = content


class ReceivedPackage(Package):
    def __init__(self, content, sender):
        super().__init__(content)
        self.sender = sender
