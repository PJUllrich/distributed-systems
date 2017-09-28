class Category:
    NAME = None
    MCAST_ADDR = None
    MCAST_PORT = None


class Temperature(Category):
    NAME = 'TEMP'
    MCAST_ADDR = '224.1.1.1'
    MCAST_PORT = 6000
    STARTING_PORT = 6001
