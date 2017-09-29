import json
import logging

import destinator.const.messages as messages
import destinator.util.util as util
from destinator.factories.message_factory import MessageFactory
from destinator.handlers.base_handler import BaseHandler

logger = logging.getLogger(__name__)


class Discovery(BaseHandler):
    def __init__(self, parent_handler):
        super().__init__(parent_handler)
        self.identifer = util.identifier()

    def start_discovery(self):
        """
        Creates a new Vector information containing information about the
        VectorTimestamp object.
        Sends out a DISCOVERY message in order to discover other active processes in the
        multicast group.
        """
        self.send_discover_message()

    def send_discover_message(self):
        """
        Sends out a DISCOVERY message in order to discover other active processes in the
        multicast group.
        """
        if self.parent.leader:
            self.end_discovery()
            return

        msg = self.identifer
        self.parent.send(messages.DISCOVERY, msg, increment=False)

    def end_discovery(self):
        """
        Ends the discovery and calls the end_discovery function of the Root handler,
        which switches the active handler from the Discovery to the VectorTimestamp
        handler.
        """
        self.parent.end_discovery()

    def handle(self, msg):
        """
        Overwrites the handle function from the BaseHandler parent class. Calls the
        super handle function with the message. Checks afterwards whether the received
        message was a DISCOVERY_RESPONSE message. Ends discovery, if yes.
        Parameters
        ----------
        msg:    str
            The incoming message in JSON format
        """
        super().handle(msg)

        vector, message_type, payload = MessageFactory.unpack(msg)
        if not message_type == messages.DISCOVERY_RESPONSE:
            logger.debug("Received message, but still in discovery mode")
            return

        identifier, process_id = self._unpack_payload(payload)

        if not identifier == self.identifer:
            logger.info(f"Received discovery response message, but not intent "
                        f"for me. My identifier {self.identifer} vs {identifier}")
            return

        logger.info(f"Received relevant discovery response message. I am "
                    f"process {process_id}")
        self.parent.vector.process_id = process_id
        # Initial port is -1, so remove it from vector
        if -1 in self.parent.vector.index:
            self.parent.vector.index.pop(-1)

        self.parent.connector.start_individual_listener(process_id)

        self.end_discovery()

    def _unpack_payload(self, payload):
        data = json.loads(payload)
        identifier = data.get(self.FIELD_IDENTIFIER)
        process_id = data.get(self.FIELD_PROCESS)
        return identifier, process_id
