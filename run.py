import destinator.const.groups as group
from destinator.device import Device
from destinator.logger import setup_logger

COUNT_DEVICES = 10

if __name__ == '__main__':
    setup_logger('output.log')

    devices = [Device(group.Temperature) for _ in range(9)]
    [device.start() for device in devices]

    devices[0].broadcast('Hello World')
