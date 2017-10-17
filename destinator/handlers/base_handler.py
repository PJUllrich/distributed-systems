import json
import logging
import threading
from abc import ABC

import destinator.const.messages as messages
from destinator.handlers.bully import Bully
from destinator.handlers.phase_king import PhaseKing
from destinator.util.package import UnpackedPackage
from destinator.util.vector import Vector

logger = logging.getLogger(__name__)


class BaseHandler(ABC):
    FIELD_IDENTIFIER = "IDENTIFY"
    FIELD_PROCESS = "PROCESS"

    def __init__(self, message_handler):
        self.parent = message_handler

        self.hold_back = []

        self.ports_identifier = {}

        self.handler_bully = Bully(self.parent)
        self.handler_phase_king = PhaseKing(self.parent)
        self.handlers = {
            messages.DISCOVERY: self.handle_discovery_msg,
            messages.DISCOVERY_RESPONSE: self.handle_discovery_msg_response,

            messages.ELECTION: self.handler_bully.handle_election,
            messages.VOTE: self.handler_bully.handle_vote,
            messages.COORDINATOR: self.handler_bully.handle_coordinate,

            messages.VT_REQUEST: self.handle_msg_request,
            messages.VT_FOUND: self.handle_msg_request_found,
            messages.VT_NOT_FOUND: self.handle_msg_request_not_found,

            messages.PHASE_KING_INIT: self.handler_phase_king.handle_init,
            messages.PHASE_KING_FOUND: self.handler_phase_king.handle_found,
            messages.PHASE_KING_SEND: self.handler_phase_king.handle_send,
            messages.PHASE_KING_DECISION: self.handler_phase_king.handle_decision,
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
        if self.parent.is_leader:
            if -1 in package.vector.index:
                logger.warning((f"Received invalid vector {package.vector.index} "
                                f"My vector is {self.parent.vector.index}"))

        handle_function = self.handlers.get(package.message_type, self.handle_default)
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

        if not self.parent.is_leader:
            logger.debug("Received DISCOVERY message, but ignoring it [i am not a "
                         "leader]")
            return

        my_port = self.parent.connector.port

        used_ports = self.parent.vector.index.keys()
        assigned_port = self.ports_identifier.get(package.payload,
                                                  list(used_ports)[-1] + 1)
        self.ports_identifier[package.payload] = assigned_port

        self.parent.vector.index[assigned_port] = 0

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

        self.parent.send(messages.DISCOVERY_RESPONSE, msg)

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

        # save all discovery response messages (avoid re-assigning of process ids)
        from destinator.handlers.discovery import Discovery
        identifier, process_id = Discovery.unpack_payload(package.payload)
        self.ports_identifier[identifier] = process_id

        self.parent.vector.index.update(package.vector.index)

        logger.info(f"Thread {threading.get_ident()}: "
                    f"Process received DISCOVERY_RESPONSE and added Process: "
                    f"{package.vector.process_id}. New index: {self.parent.vector.index}")

    def handle_msg_request(self, package):
        """
        Handles a request for an old package that was sent, but apparently was missed by
        the requesting process. Retrieves the package that was sent and sends it to the
        process directly.

        Parameters
        ----------
        package: JsonPackage
        """

        logger.debug(f"Received old message request from process: {package.sender} for "
                     f"message: {package.payload}")
        requested_msg_id = int(package.payload)
        old_msg = self.parent.get_old_message(requested_msg_id)

        if old_msg is None:
            logger.debug(f"Could not find message {requested_msg_id} in history")
            self.parent.send(messages.VT_NOT_FOUND, requested_msg_id, package.sender)
            return

        logger.debug(f"Sent back old message {requested_msg_id} to {package.sender}")
        self.parent.send(messages.VT_FOUND, old_msg, package.sender)

    def handle_msg_request_found(self, package):
        """
        Handles the return message of a process from which an old message was requested
        again since it appeared to be missing in the hold back Queue.

        Parameters
        ----------
        package: JsonPackage
        """

        logger.debug(f"Received requested message from process {package.sender}")
        self.hold_back.append(package.payload)

    def handle_msg_request_not_found(self, package):
        """
        Handles a return messages indicating that a process from which an old messages
        was requestsed again could not find the message in its sent history.

        Creates a substitution message, that substitutes the missing message and adds a
        default value of 15 as payload.

        Parameters
        ----------
        package: JsonPackage
        """

        logger.debug(f"Received requested message NOT found from process "
                     f"{package.sender}. Will substitute missing message.")
        vector_substitute = Vector.create_vector(self.parent.vector.group_id,
                                                 package.sender)
        package_substitute = UnpackedPackage(vector_substitute, messages.TEMPERATURE, 15)

        self.hold_back.append(package_substitute)

        if self.parent.vector.index.get(package.sender) < package.payload:
            self.parent.vector.index.update({package.sender: package.payload})

    def handle_default(self, package):
        """
        The default function to handle incoming messages. At the moment, only logs the
        reception of the message.

        Parameters
        ----------
        package: JsonPackage
            The incoming package
        """
        if package.message_type == messages.TEMPERATURE:
            self.hold_back.append(package)
            self.parent.vector.index.update(package.vector.index)
        else:
            logger.debug(f"Handled package {package.message_type} - {package.payload} "
                         f"with default handler")
