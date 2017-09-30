import logging
import destinator.const.messages as messages

logger = logging.getLogger(__name__)

class Bully:
    def __init__(self, parent_handler):
        self.parent = parent_handler

    def call_for_election(self):
        my_process_id = self.parent.vector.process_id
        process_ids = self.parent.vector.index.keys()

        higher_processes = [x for x in process_ids if my_process_id < x]
        for process_id in higher_processes:
            self.parent.send(messages.ELECTION, "hi", process_id)

    def handle_election(self, package):
        logger.warning(f"Received election message from {package.vector.process_id}")

    def handle_vote(self, package):
        logger.warning(f"Received vote message from {package.vector.process_id}")

    def handle_coordinate(self, package):
        logger.warning(f"Received coordinate message from {package.vector.process_id}")
