import json
import logging
import threading

import destinator.const.messages as messages

logger = logging.getLogger(__name__)


class Discovery:
    FIELD_IDENTIFIER = "IDENTIFY"
    FIELD_PROCESS = "PROCESS"

    JOB_ID = 'DISCOVERY_JOB'
    JOB_TIMEOUT = 15

    def __init__(self, parent_handler):
        self.parent = parent_handler

        self.ports_identifier = {}

        self.job = self.parent.scheduler.add_job(self.send_discovery_message, 'interval',
                                                 minutes=self.JOB_TIMEOUT / 60,
                                                 replace_existing=True, id=self.JOB_ID)
        self.job.pause()

        if self.parent.is_discovered is not True:
            self.start_discovery()

    def start_discovery(self):
        """
        Creates a new Vector information containing information about the
        VectorTimestamp object.
        Sends out a DISCOVERY message in order to discover other active processes in the
        multicast group.
        """
        self.send_discovery_message()
        self.job.resume()

    def send_discovery_message(self):
        """
        Sends out a DISCOVERY message in order to discover other active processes in the
        multicast group.
        """
        if self.parent.leader:
            self.end_discovery()
            return

        msg = self.parent.identifier
        self.parent.send(messages.DISCOVERY, msg, increment=False)

    def end_discovery(self):
        """
        Ends the discovery and calls the end_discovery function of the Root handler,
        which switches the active handler from the Discovery to the VectorTimestamp
        handler.
        """
        self.job.pause()
        self.parent.end_discovery()

    def handle_discovery(self, package):
        """
        Adds a Process ID to the Vector index if the index does not yet contain the
        Process ID.

        Sends a response to a DISCOVERY message containing identifying information
        about the VectorTimestamp object.

        Parameters
        ----------
        package: JsonPackage
            The incoming package
        """
        if not package.message_type == messages.DISCOVERY:
            logger.warning(f'discovery function was called for the wrong '
                           f'message text {package.message_type}')
            return

        if not self.parent.leader:
            logger.debug("Received DISCOVERY message, but ignoring it [i am not a "
                         "leader]")
            return

        my_port = self.parent.connector.port

        used_ports = self.parent.vector.index.keys()
        assigned_port = self.ports_identifier.get(package.payload,
                                                  list(used_ports)[-1] + 1)
        self.ports_identifier[package.payload] = assigned_port

        self.parent.vector.index[assigned_port] = \
            self.parent.vector.index.get(my_port)

        logger.debug(f"Got discover message from {package.payload}. "
                     f"My port {my_port}. "
                     f"Found devices: {used_ports}. "
                     f"Assigning port {assigned_port} to new device. "
                     f"My vector is {self.parent.vector.index.get(my_port)}")

        logger.info(f"Leader added Process: {assigned_port}. "
                    f"New index: {self.parent.vector.index}")

        data = {
            self.FIELD_PROCESS: assigned_port,
            self.FIELD_IDENTIFIER: package.payload
        }
        msg = json.dumps(data)

        self.parent.send(messages.DISCOVERY_RESPONSE, msg, increment=False)

    def handle_discovery_response(self, package):
        """
        Handles a DISCOVERY_RESPONSE message. Adds any Process IDs to the own Vector
        index and updates the message counts of the existing Process IDs.

        Parameters
        ----------
        package: JsonPackage
            The incoming package
        """
        if not package.message_type == messages.DISCOVERY_RESPONSE:
            logger.warning(f'discovery_response function was called for the wrong '
                           f'message text {package.message_type}')
            return

        # save all discovery response messages (avoid re-assigning of process ids)
        identifier, process_id = self._unpack_payload(package.payload)
        self.ports_identifier[identifier] = process_id
        # This reset the vector according to the leader
        self.parent.vector.index.update(package.vector.index)

        logger.info(f"Thread {threading.get_ident()}: "
                    f"Added Process: {process_id}. New index:  "
                    f"{self.parent.vector.index}")
        logger.debug(f"Thread {threading.get_ident()}: {self.ports_identifier}")

        if self.parent.is_discovered is False:
            self.handle_own_discovery_response(package)

    def handle_own_discovery_response(self, package):
        identifier, process_id = self._unpack_payload(package.payload)
        if not identifier == self.parent.identifier:
            logger.info(f"Received discovery response message, but not intent "
                        f"for me. My identifier {self.parent.identifier} vs {identifier}")
            return

        logger.info(f"Received relevant discovery response message. I am "
                    f"process {process_id}")
        self.parent.vector.process_id = process_id
        # Initial port is -1, so remove it from vector
        if -1 in self.parent.vector.index:
            self.parent.vector.index.pop(-1)

        self.parent.connector.start_individual_listener(process_id)

        self.end_discovery()

    @classmethod
    def _unpack_payload(cls, payload):
        data = json.loads(payload)
        identifier = data.get(cls.FIELD_IDENTIFIER)
        process_id = data.get(cls.FIELD_PROCESS)
        return identifier, process_id
