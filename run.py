from destinator.device import Device
import destinator.const.groups as group

receiver = Device(group.Temperature)
sender = Device(group.Temperature)
sender.broadcast('Hello World')
