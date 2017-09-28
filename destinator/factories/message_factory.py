import json

from destinator.util.vector import Vector


class MessageFactory:
    FIELD_VECTOR = "VECTOR"
    FIELD_PAYLOAD = "PAYLOAD"
    FIELD_TYPE = "TYPE"

    @classmethod
    def pack(cls, vector, payload, message_type):
        """
        Packs a Vector object and a text into a JSON string.

        Parameters
        ----------
        vector: Vector
            A Vector object containing identifying information about a VectorTimestamp
            object.

        text:   str
            A String that will be packed together with the Vector data

        Returns
        -------
        str
            JSON data of the input
        """
        data = {
            cls.FIELD_VECTOR: vector.__dict__,
            cls.FIELD_PAYLOAD: payload,
            cls.FIELD_TYPE: message_type
        }
        return json.dumps(data)

    @classmethod
    def unpack(cls, msg):
        """
        Creates a dict from the JSON inpud.
        Retrieves JSON data about the Vector object from the Sender and the text that
        was sent with the message.
        Creates a new Vector object from the retrieved JSON data.

        Parameters
        ----------
        msg:    str
            JSON data

        Returns
        -------
        Vector
            A Vector object containing identifying information about the Sender of the
            JSON data input.
        str
            The text that was sent together with the Vector data.
        """
        data = json.loads(msg)

        vector_json = data.get(cls.FIELD_VECTOR)
        vector = Vector.from_json(vector_json)

        payload = data.get(cls.FIELD_PAYLOAD)
        message_type = data.get(cls.FIELD_TYPE)

        return vector, payload, message_type
