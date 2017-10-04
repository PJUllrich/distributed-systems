import logging
import random
import time

import destinator.const.groups as group
from destinator.device import Device
from destinator.util.logger import setup_logger

logger = logging.getLogger(__name__)

# input area
COUNT_DEVICES = 10
ACTIVE_THREADS = 3
PROB_CREATE = 0.4
PROB_CRASH = 0.2
SLEEP_TIME = 1


def span_device():
    logger.info("Letting one device crash...")
    device_index = random.randint(0, len(devices) - 1)
    devices[device_index].cancelled = True
    devices.pop(device_index)


def kill_device():
    logger.info("Starting up new device")
    devices.append(Device(group.Temperature).start())


if __name__ == '__main__':
    setup_logger('output.log')

    leader = Device(group.Temperature)
    leader.set_leader(True)

    devices = [leader] + [Device(group.Temperature) for _ in range(ACTIVE_THREADS - 1)]
    [device.start() for device in devices]

    time.sleep(6)

    while True:
        if len(devices) is not 0 and random.random() < PROB_CRASH:
            span_device()
        if random.random() < PROB_CREATE:
            kill_device()

        time.sleep(SLEEP_TIME)
