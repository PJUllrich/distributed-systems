import json
import logging

import destinator.const.messages as messages
import destinator.util.util as util
from destinator.handlers.base_handler import BaseHandler

logger = logging.getLogger(__name__)


class Discovery(BaseHandler):
    JOB_ID = 'DISCOVERY_JOB'
    JOB_TIMEOUT = 15

    def __init__(self, parent_handler):
        super().__init__(parent_handler)
        self.identifier = util.identifier()

        self.job = self.parent.scheduler.add_job(self.send_discover_message, 'interval',
                                                 minutes=self.JOB_TIMEOUT / 60,
                                                 replace_existing=True, id=self.JOB_ID)
        self.job.pause()

    def start_discovery(self):
        """
        Creates a new Vector information containing information about the
        VectorTimestamp object.
        Sends out a DISCOVERY message in order to discover other active processes in the
        multicast group.
        """
        self.send_discover_message()
        self.job.resume()

    def send_discover_message(self):
        """
        Sends out a DISCOVERY message in order to discover other active processes in the
        multicast group.
        """
        if self.parent.is_leader:
            self.end_discovery()
            return

        msg = self.identifier
        self.parent.send(messages.DISCOVERY, msg)

    def end_discovery(self):
        """
        Ends the discovery and calls the end_discovery function of the Root handler,
        which switches the active handler from the Discovery to the VectorTimestamp
        handler.
        """
        self.job.pause()
        self.parent.end_discovery()

    def handle(self, package):
        """
        Overwrites the handle function from the BaseHandler parent class. Calls the
        super handle function with the message. Checks afterwards whether the received
        message was a DISCOVERY_RESPONSE message. Ends discovery, if yes.
        Parameters
        ----------
        package: JsonPackage
            The incoming package
        """
        super().handle(package)

        if not package.message_type == messages.DISCOVERY_RESPONSE:
            logger.debug("Received message, but still in discovery mode")
            return

        identifier, process_id = self.unpack_payload(package.payload)

        if not identifier == self.identifier:
            logger.info(f"Received discovery response message, but not intent "
                        f"for me. My identifier {self.identifier} vs {identifier}")
            return

        logger.info(f"Received relevant discovery response message. I am "
                    f"process {process_id}. Ident: {self.identifier}")

        if self.parent.vector.process_id != -1:
            logger.warning(f"P {self.parent.vector.process_id} getting assigned a new "
                           f"port {process_id}")
        self.parent.vector.process_id = process_id
        # Initial port is -1, so remove it from vector
        if -1 in self.parent.vector.index:
            self.parent.vector.index.pop(-1)

        self.parent.connector.start_individual_listener(process_id)

        self.end_discovery()

    @classmethod
    def unpack_payload(cls, payload):
        data = json.loads(payload)
        identifier = data.get(cls.FIELD_IDENTIFIER)
        process_id = data.get(cls.FIELD_PROCESS)
        return identifier, process_id
