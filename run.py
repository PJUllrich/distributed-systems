import destinator.const.groups as group
from destinator.device import Device
from destinator.logger import setup_logger

if __name__ == '__main__':
    setup_logger('output.log')

    [Device(group.Temperature) for _ in range(9)]
    sender = Device(group.Temperature)
    sender.broadcast('Hello World')
