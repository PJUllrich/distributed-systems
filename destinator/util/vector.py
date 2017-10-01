class Vector:
    def __init__(self, group_id, process_id, index):
        self.group_id = group_id
        self.process_id = process_id
        self.index = index

    @staticmethod
    def create_vector(group_id, process_id):
        """
        Creates a new Vector object with the group id and process id of the Connector
        object. Sets the counter for own messages to 0.

        Returns
        -------
        Vector
            A new Vector object holding information about Group ID, Process ID,
            and own message count

        """
        id_group_own = group_id
        id_process_id_own = process_id
        id_message_own = 0

        index = {
            id_process_id_own: id_message_own
        }

        return Vector(id_group_own, id_process_id_own, index)

    def __eq__(self, other):
        return other.process_id == self.process_id

    @staticmethod
    def from_json(data):
        """
        Creates a Vector object from JSON data

        Parameters
        ----------
        data:   dict
            String containing JSON data

        Returns
        -------
        Vector
            A new Vector object with the Group ID, Process ID, and index retrieved from
            the JSON data
        """
        group_id = data["group_id"]
        process_id = data["process_id"]
        index = {int(k): int(v) for k, v in data["index"].items()}

        return Vector(group_id, process_id, index)
