class Device:
    """Base class for Nodes"""

    def __init__(self):
        pass

    def broadcast(self):
        pass

    def receive(self, msg):
        pass

    def deliver(self, msg):
        pass
