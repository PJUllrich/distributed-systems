import destinator.const.messages as messages


class Category:
    NAME = None
    MCAST_ADDR = None
    MCAST_PORT = None


class Temperature(Category):
    NAME = messages.TEMPERATURE
    MCAST_ADDR = '224.1.1.1'
    MCAST_PORT = 6000
    STARTING_PORT = 6001
