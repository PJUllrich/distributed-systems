import threading
import uuid


def identifier():
    """
    Returns a unique identifier based on the network mac address and thread id
    """
    mac_addr = hex(uuid.getnode()).replace('0x', '')
    thread_id = threading.get_ident()

    return f"{mac_addr}-{thread_id}"
