import logging

import destinator.const.messages as messages
from destinator.handlers.base_handler import BaseHandler

logger = logging.getLogger(__name__)


class VectorTimestamp(BaseHandler):
    def __init__(self, parent_handler):
        super().__init__(parent_handler)

    def handle_default(self, package):
        """
        Overwrites the BaseHandler default function. Packs the Vector and text input
        pack into a message and forwards it to the b_discover function.

        Parameters
        ----------
        package: Package
            The received package
        """

        self.b_deliver(package)

    def co_multicast(self, package):
        """
        Sends out a text together with the Vector object of the VectorTimestamp object.
        Before sending, the message counter for the Process sending is incremented by 1
        unless the Process is discovering.

        Messages are sent through the Connector class. Look at Connector.send() for
        more specifications of where the message is sent.

        Parameters
        ----------
        text:   str
            The text to be sent
        """

        self.parent.send(package)

    def b_deliver(self, package):
        """
        Handles the ordering of messages before delivering them. This function is (
        alomost) alike to the function as described in the Lecture Slides. Only
        difference is that the function checks whether a received message is actually
        new, or was seen before already, in which case it ignores the message.

        Parameters
        ----------
        package:    Package
            The package received
        """
        if self.is_old(package.vector):
            return

        self.hold_back.append(package)

        deliverables = self.get_deliverables()
        for package in deliverables:
            self.co_deliver(package)
            self.increment(package.vector)

    def co_deliver(self, package):
        """
        Delivers a message to the Connector's Queue shared with a Device object

        Parameters
        ----------
        msg:    str
            A String containing the message (Identifier + text) in JSON format

        """
        self.parent.deliver(package)

    def is_old(self, vector):
        """
        Checks whether a message was already seen, by comparing the message counter of
        the Process which sent the message with the message counter of that Process in
        the Device's counter dict.

        Parameters
        ----------
        vector: Vector
            The Vector object sent with the message

        Returns
        -------
        bool
            True if message was seen already, False if message is new.
        """
        idx_new = vector.index.get(vector.process_id)
        idx_old = self.parent.vector.index.get(vector.process_id, 0)

        return idx_new <= idx_old

    def get_deliverables(self):
        """
        Iterates over the Hold-back Queue and checks whether a message is causally
        following the previously seen messages.

        Returns
        -------
        [Package]
            An array with the packages that are causally following the
            previously seen messages. The Ordering in which the messages should be
            delivered is preserved in this array.
        """
        out = []
        more = True

        while more:
            self.hold_back = [p for p in self.hold_back if p not in out]
            more = False

            for package in reversed(self.hold_back):
                if self.is_causal(package.vector):
                    out.append(package)
                    more = True

        return out

    def is_causal(self, vector):
        """
        Checks whether a Vector is causally following previously seen messages
        according to the specifications described in the Lecture.

        Parameters
        ----------
        vector: Vector
            The Vector whose causality should be checked.

        Returns
        -------
        bool
            Whether the Vector causally follows previously seen messages or not.

        """
        v_new = vector.index.get(vector.process_id)
        v_old = self.parent.vector.index.get(vector.process_id, 0)

        if not v_new == v_old + 1:
            self.request_missed_messages(v_old, v_new, vector.process_id)
            return False

        for k, val_jk in vector.index.items():
            val_ik = self.parent.vector.index.get(k, 0)
            if not val_jk <= val_ik and not k == vector.process_id:
                return False

        return True

    def increment(self, vector):
        """
        Increments the message counter of a Process in the local Vector index by 1.

        Parameters
        ----------
        vector: Vector
            The Vector which message counter should be incremented
        """
        val_new = self.parent.vector.index.get(vector.process_id, 0) + 1
        self.parent.vector.index.update({vector.process_id: val_new})

    def request_missed_messages(self, id_own, id_new, process_id):
        """

        Parameters
        ----------
        id_own: int
            The message count of the process_id in the own Vector.index
        id_new: int
            The message count of the process_id received from that process
        process_id: int
            The process id of the process from which the messages shall be requested
        """

        for msg_id in range(id_own + 1, id_new):
            logger.debug(f"Requested {id_new - id_own + 1} messages from process: "
                         f"{process_id}")
            self.parent.send(messages.REQUEST_MESSAGE, msg_id, process_id)
