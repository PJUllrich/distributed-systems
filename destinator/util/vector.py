class Vector:
    def __init__(self, group_id, process_id, index):
        self.group_id = group_id
        self.process_id = process_id
        self.index = index

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
