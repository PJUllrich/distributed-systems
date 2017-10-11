import logging
from functools import wraps

import destinator.const.messages as messages
from destinator.factories.message_factory import MessageFactory

logger = logging.getLogger(__name__)


def verify_message(func):
    @wraps(func)
    def wrapper(obj, package):
        """
        Validates a received JsonPackage
        Parameters
        ----------
        package: JsonPackage
        """
        # Ignore own messages
        if (package.vector.group_id == obj.vector.group_id
            and package.vector.process_id == obj.vector.process_id):
            return

        # Ignore messages coming from different groups. Log their reception.
        if package.vector.group_id != obj.vector.group_id:
            logger.warning(
                f"Received message from different group {package.vector.group_id}, "
                f"payload: {package.payload}")
            return

        # Ignore DISCOVERY messages if not the leader
        if package.message_type == messages.DISCOVERY and not obj.leader:
            return

        return func(obj, package)

    return wrapper
