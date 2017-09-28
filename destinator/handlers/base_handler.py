import json
import threading
from abc import ABC
import logging

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
        vector, payload, message_type = MessageFactory.unpack(msg)
        handle_function = self.handlers.get(message_type, self.handle_unknown)
        handle_function(vector, payload, message_type)

    def handle_discovery_msg(self, vector, payload, message_type):
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
        if self.parent.leader:
            my_port = self.parent.connector.port
            used_ports = self.parent.vector.index.keys()
            assigned_port = list(used_ports)[-1] + 1
            vector.process_id = assigned_port
            logger.debug(f"Got discover message from {payload}")
            logger.debug(f"My port {my_port}")
            logger.debug(f"Found devices: {used_ports}")
            logger.debug(f"Assigning port {assigned_port} to new device")
            logger.debug(f"My vector is {self.parent.vector.index.get(my_port)}")
            self.parent.vector.index[assigned_port] = \
                self.parent.vector.index.get(my_port)

            logger.info(f"Thread {threading.get_ident()}: "
                        f"Leader added Process: {vector.process_id}. "
                        f"New index: {self.parent.vector.index}")

            data = {
                self.FIELD_IDENTIFIER: payload,
                self.FIELD_PROCESS: assigned_port
            }
            msg = json.dumps(data)

            self.parent.send(msg, messages.DISCOVERY_RESPONSE, increment=False)
        else:
            logger.debug("Received DISCOVERY message, but ignoring it [i am not a "
                         "leader]")

    def handle_discovery_msg_response(self, vector, payload, message_type):
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

    def handle_unknown(self, vector, payload, message_type):
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
