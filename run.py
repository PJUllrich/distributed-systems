import random
import time

import destinator.const.groups as group
from destinator.device import Device
from destinator.util.logger import setup_logger

# input area
COUNT_DEVICES = 10
ACTIVE_THREADS = 3
PROB_CREATE = 0.00001
PROB_CRASH = 0.2
SLEEP_TIME = 1

if __name__ == '__main__':
    setup_logger('output.log')

    leader = Device(group.Temperature)
    leader.communicator.message_handler.leader = True

    devices = [leader] + [Device(group.Temperature) for _ in range(ACTIVE_THREADS - 1)]
    [device.start() for device in devices]

    time.sleep(6)

    while True:
        prob = random.random()

        if prob < (PROB_CRASH ):
            devices[random.randint(0, ACTIVE_THREADS - 1)].cancelled = True
        elif prob > 1 - (PROB_CREATE / 1000):
            devices.append(Device(group.Temperature).start())

        time.sleep(SLEEP_TIME)
