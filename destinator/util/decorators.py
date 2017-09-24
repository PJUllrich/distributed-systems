import logging
from functools import wraps

from destinator.factories.message_factory import MessageFactory

logger = logging.getLogger(__name__)


def verify_message(func):
    @wraps(func)
    def wrapper(obj, msg):
        vector, text = MessageFactory.unpack(msg)

        # Ignore own messages
        if (vector.group_id == obj.vector.group_id
            and vector.process_id == obj.vector.process_id):
            return

        # Ignore messages coming from different groups. Log their reception.
        if vector.group_id != obj.vector.group_id:
            logger.warning(
                f"Received message from different group {vector.group_id}, text: {text}")
            return

        return func(obj, msg)

    return wrapper
