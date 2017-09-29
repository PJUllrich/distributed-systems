import json
import logging
import threading
from abc import ABC

import destinator.const.messages as messages
from destinator.factories.message_factory import MessageFactory

logger = logging.getLogger(__name__)


class BaseHandler(ABC):
    FIELD_IDENTIFIER = "IDENTIFY"
    FIELD_PROCESS = "PROCESS"

    def __init__(self, message_handler):
        self.parent = message_handler
        self.handlers = {
            messages.DISCOVERY: self.handle_discovery_msg,
            messages.DISCOVERY_RESPONSE: self.handle_discovery_msg_response
        }

    def handle(self, msg):
        """
        Default function for handling messages. Looks up a function in the handlers
        dict and executes the function if an applicable one is found. Otherwise calls
        the default function, which can be overwritten by sub-classes to add their own
        functionality.

        Parameters
        ----------
        msg:    str
            Received JSON data
        """
        vector, message_type, payload = MessageFactory.unpack(msg)

        if self.parent.leader:
            if -1 in vector.index:
                logger.warning(f"Received invalid vector {vector.index}")
                logger.warning(f"My vector is {self.parent.vector.index}")

        handle_function = self.handlers.get(message_type, self.handle_unknown)
        handle_function(vector, message_type, payload)

    def handle_discovery_msg(self, vector, message_type, payload):
        """
        Adds a Process ID to the Vector index if the index does not yet contain the
        Process ID.

        Sends a response to a DISCOVERY message containing identifying information
        about the VectorTimestamp object.

        Parameters
        ----------
        vector: Vector
            The Vector object received with the message
        payload: str
            The message text, should be 'DISCOVERY_RESPONSE'
        message_type: str
            The group of the message
        """
        if not message_type == messages.DISCOVERY:
            logger.warning(f'discovery function was called for the wrong '
                           f'message text {message_type}')
            return

        if not self.parent.leader:
            logger.debug("Received DISCOVERY message, but ignoring it [i am not a "
                         "leader]")
            return

        my_port = self.parent.connector.port
        used_ports = self.parent.vector.index.keys()
        assigned_port = list(used_ports)[-1] + 1
        vector.process_id = assigned_port
        logger.debug(f"Got discover message from {payload}. "
                     f"My port {my_port}. "
                     f"Found devices: {used_ports}. "
                     f"Assigning port {assigned_port} to new device. "
                     f"My vector is {self.parent.vector.index.get(my_port)}")
        self.parent.vector.index[assigned_port] = \
            self.parent.vector.index.get(my_port)

        logger.info(f"Thread {threading.get_ident()}: "
                    f"Leader added Process: {vector.process_id}. "
                    f"New index: {self.parent.vector.index}")

        data = {
            self.FIELD_PROCESS: assigned_port,
            self.FIELD_IDENTIFIER: payload
        }
        msg = json.dumps(data)

        self.parent.send(messages.DISCOVERY_RESPONSE, msg, increment=False)

    def handle_discovery_msg_response(self, vector, message_type, payload):
        """
        Handles a DISCOVERY_RESPONSE message. Adds any Process IDs to the own Vector
        index and updates the message counts of the existing Process IDs.

        Parameters
        ----------
        vector: Vector
            The Vector object received with the message
        payload: str
            The message text, should be 'DISCOVERY_RESPONSE'
        message_type: str
            The group of the message
        """
        if not message_type == messages.DISCOVERY_RESPONSE:
            logger.warning(f'discovery_response function was called for the wrong '
                           f'message text {message_type}')
            return

        self.parent.vector.index.update(vector.index)

        logger.info(f"Thread {threading.get_ident()}: "
                    f"Process received DISCOVERY_RESPONSE and added Process: "
                    f"{vector.process_id}. New index: {self.parent.vector.index}")

    def handle_unknown(self, vector, message_type, payload):
        """
        The default function to handle incoming messages. At the moment, only logs the
        reception of the message.

        Parameters
        ----------
        vector: Vector
            The Vector object received with the message
        payload: str
            The message text received with the message
        message_type: str
            The group of the message
        """
        logger.warning(f"Received a message {message_type} with payload {payload} "
                    f"for which no handler exists")
        self.parent.vector.index.update(vector.index)
