import random
import time

import destinator.const.groups as group
from destinator.device import Device
from destinator.util.logger import setup_logger

COUNT_DEVICES = 10
ACTIVE_THREADS = 3

if __name__ == '__main__':
    setup_logger('output.log')

    devices = [Device(group.Temperature) for _ in range(ACTIVE_THREADS)]
    [device.start() for device in devices]

    time.sleep(6)
    while True:
        msg = f'Temperature is: - {random.randint(1, 30)}'
        devices[random.randint(0, ACTIVE_THREADS-1)].send(msg)
        time.sleep(random.randint(3, 6))
