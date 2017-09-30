import json
import logging
import threading
from abc import ABC

import destinator.const.messages as messages
from destinator.factories.message_factory import MessageFactory
from destinator.handlers.bully import Bully

logger = logging.getLogger(__name__)


class BaseHandler(ABC):
    FIELD_IDENTIFIER = "IDENTIFY"
    FIELD_PROCESS = "PROCESS"

    def __init__(self, message_handler):
        self.parent = message_handler

        self.handler_bully = Bully(self.parent)
        self.handlers = {
            messages.DISCOVERY: self.handle_discovery_msg,
            messages.DISCOVERY_RESPONSE: self.handle_discovery_msg_response,

            messages.ELECTION: self.handler_bully.handle_election,
            messages.VOTE: self.handler_bully.handle_vote,
            messages.COORDINATOR: self.handler_bully.handle_coordinate,
        }

    def handle(self, package):
        """
        Default function for handling messages. Looks up a function in the handlers
        dict and executes the function if an applicable one is found. Otherwise calls
        the default function, which can be overwritten by sub-classes to add their own
        functionality.

        Parameters
        ----------
        package: JsonPackage
            The incoming package
        """
        if self.parent.leader:
            if -1 in package.vector.index:
                logger.warning(f"Received invalid vector {package.vector.index}")
                logger.warning(f"My vector is {self.parent.vector.index}")

        handle_function = self.handlers.get(package.message_type, self.handle_unknown)
        handle_function(package)

    def handle_discovery_msg(self, package):
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
        assigned_port = list(used_ports)[-1] + 1
        package.vector.process_id = assigned_port  # TODO is this line necessary
        logger.debug(f"Got discover message from {package.payload}. "
                     f"My port {my_port}. "
                     f"Found devices: {used_ports}. "
                     f"Assigning port {assigned_port} to new device. "
                     f"My vector is {self.parent.vector.index.get(my_port)}")
        self.parent.vector.index[assigned_port] = \
            self.parent.vector.index.get(my_port)

        logger.info(f"Thread {threading.get_ident()}: "
                    f"Leader added Process: {package.vector.process_id}. "
                    f"New index: {self.parent.vector.index}")

        data = {
            self.FIELD_PROCESS: assigned_port,
            self.FIELD_IDENTIFIER: package.payload
        }
        msg = json.dumps(data)

        self.parent.send(messages.DISCOVERY_RESPONSE, msg, increment=False)

    def handle_discovery_msg_response(self, package):
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

        self.parent.vector.index.update(package.vector.index)

        logger.info(f"Thread {threading.get_ident()}: "
                    f"Process received DISCOVERY_RESPONSE and added Process: "
                    f"{package.vector.process_id}. New index: {self.parent.vector.index}")

    def handle_unknown(self, package):
        """
        The default function to handle incoming messages. At the moment, only logs the
        reception of the message.

        Parameters
        ----------
        package: JsonPackage
            The incoming package
        """
        logger.warning(f"Received a message {package.message_type} with payload "
                       f"{package.payload} for which no handler exists")
        self.parent.vector.index.update(package.vector.index)
