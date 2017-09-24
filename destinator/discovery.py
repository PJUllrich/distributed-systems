import logging
import threading
import time

import destinator.const.messages as messages
from destinator.message_factory import MessageFactory

logger = logging.getLogger(__name__)

# Time (in seconds) a process will wait until stopping to discover new Processes
DISCOVERY_TIMEOUT = 10


class Discovery:
    def __init__(self, vector_timestamp):
        self.vt = vector_timestamp
        self.discovering = False
        self.discovery_start = None

    def start_discovery(self):
        """
        Creates a new Vector information containing information about the
        VectorTimestamp object.

        Sends out a DISCOVERY message in order to discover other active processes in the
        multicast group.
        """

        self.discovering = True
        self.vt.co_multicast(messages.DISCOVERY)
        self.discovery_start = time.time()

    def respond_discovery(self):
        """
        Sends a response to a DISCOVERY message containing identifying information
        about the VectorTimestamp object.
        """
        self.vt.co_multicast(messages.DISCOVERY_RESPONSE)

    def handle_discovery(self, msg):
        """
        Adds a Process ID to the Vector index of the VectorTimestamp object when the
        Process ID is not yet in the index. Ignores the message otherwise.

        Eventually, checks whether the DISCOVERY_TIMEOUT is reached.

        Parameters
        ----------
        msg:    str
            Received JSON data
        """

        vector, text = MessageFactory.unpack(msg)

        if vector.process_id not in self.vt.vector.index:
            self.vt.vector.index[vector.process_id] = vector.index.get(vector.process_id)
            logger.info(f"Thread {threading.get_ident()}: "
                        f"VectorTimestamp added Process: {vector.process_id}."
                        f"New index: {self.vt.vector.index}")

        self.check_discovering_timeout()

    def check_discovering_timeout(self):
        """
        Checks whether DISCOVERY_TIMEOUT is reached.
        Stops the Discovery mode if True.
        """
        if time.time() - self.discovery_start >= DISCOVERY_TIMEOUT:
            self.discovering = False
            logger.debug(f"Thread {threading.get_ident()}: "
                         f"VectorTimestamp stopped Discovery Mode")
