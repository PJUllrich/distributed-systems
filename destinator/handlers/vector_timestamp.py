import logging

from destinator.factories.message_factory import MessageFactory
from destinator.handlers.base_handler import BaseHandler

logger = logging.getLogger(__name__)


class VectorTimestamp(BaseHandler):
    def __init__(self, message_handler):
        super().__init__(message_handler)

        self.hold_back = []

    def default(self, vector, text):
        """
        Overwrites the BaseHandler default function. Packs the Vector and text input
        pack into a message and forwards it to the b_discover function.

        Parameters
        ----------
        vector: Vector
            The Vector from the received message
        text:   str
            The text that was sent with the message
        """
        logger.info(f'VectorTimestamp called Default function for message: {text}')

        msg = MessageFactory.pack(vector, text)
        self.b_deliver(msg)

    def co_multicast(self, text):
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

        self.parent.send(text)

    def b_deliver(self, msg):
        """
        Handles the ordering of messages before delivering them. This function is (
        alomost) alike to the function as described in the Lecture Slides. Only
        difference is that the function checks whether a received message is actually
        new, or was seen before already, in which case it ignores the message.

        Parameters
        ----------
        msg:    str
            The message in JSON format
        """
        vector, _ = MessageFactory.unpack(msg)
        if self.is_old(vector):
            return

        self.hold_back.append([vector, msg])

        deliverables = self.get_deliverables()
        for v, m in deliverables:
            self.co_deliver(m)
            self.increment(v)

    def co_deliver(self, msg):
        """
        Delivers a message to the Connector's Queue shared with a Device object

        Parameters
        ----------
        msg:    str
            A String containing the message (Identifier + text) in JSON format

        """
        self.parent.deliver(msg)

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
        idx_j = vector.index.get(vector.process_id)
        idx_i = self.parent.vector.index.get(vector.process_id, 0)

        return idx_j <= idx_i

    def get_deliverables(self):
        """
        Iterates over the Hold-back Queue and checks whether a message is causally
        following the previously seen messages.

        Returns
        -------
        [(Vector, str)]
            An array with the Vector, Message pairs that are causally following the
            previously seen messages. The Ordering in which the messages should be
            delivered is preserved in this array.
        """
        out = []
        more = True

        while more:
            self.hold_back = [(v, m) for v, m in self.hold_back if (v, m) not in out]
            more = False

            for vector, msg in reversed(self.hold_back):
                if self.is_causal(vector):
                    out.append((vector, msg))
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
        v_j = vector.index.get(vector.process_id)
        v_i = self.parent.vector.index.get(vector.process_id, 0)

        if not v_j == v_i + 1:
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
        val_new = self.parent.vector.index.get(vector.process_id) + 1
        self.parent.vector.index.update({vector.process_id: val_new})
