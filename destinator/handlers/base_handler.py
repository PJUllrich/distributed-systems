import logging
from abc import ABC

import destinator.const.messages as messages
from destinator.handlers.bully import Bully
from destinator.handlers.discovery import Discovery

logger = logging.getLogger(__name__)


class BaseHandler(ABC):
    def __init__(self, message_handler):
        self.parent = message_handler

        self.handler_discover = Discovery(self.parent)
        self.handler_bully = Bully(self.parent)
        self.handlers = {
            messages.DISCOVERY: self.handler_discover.handle_discovery,
            messages.DISCOVERY_RESPONSE: self.handler_discover.handle_discovery_response,

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
