import logging
import time

import destinator.const.messages as messages

logger = logging.getLogger(__name__)


class Bully:
    BULLY_CALL_JOB_ID = "BULLY_JOB_CALL"
    BULLY_RESPONSE_JOB_ID = "BULLY_RESPONSE_JOB_CALL"
    BULLY_COORDINATOR_JOB_ID = "BULLY_COORDINATOR_JOB_CALL"
    CALL_TIMEOUT = 40
    RESPONSE_TIMEOUT = 10
    COORDINATE_TIMEOUT = 30

    def __init__(self, parent_handler):
        self.parent = parent_handler

        self.election_was_answered = False
        self.last_coordinator_msg = None

        self.job_call = self.parent.scheduler.add_job(
            self.call_for_election, 'interval', minutes=self.CALL_TIMEOUT / 60,
            replace_existing=True, id=self.BULLY_CALL_JOB_ID)
        self.job_call_response = self.parent.scheduler.add_job(
            self._check_election_responses, 'interval',
            minutes=self.RESPONSE_TIMEOUT / 60, replace_existing=True,
            id=self.BULLY_RESPONSE_JOB_ID)
        self.job_call_response.pause()
        self.job_coordinator = self.parent.scheduler.add_job(
            self._check_coordinator_response, 'interval',
            minutes=self.COORDINATE_TIMEOUT / 60, replace_existing=True,
            id=self.BULLY_COORDINATOR_JOB_ID)
        self.job_coordinator.pause()

        self.call_for_election()

    def call_for_election(self):
        if self.process_id <= 0:
            logger.debug(f"P {self.process_id}: Process ID is not yet set")
            return
        if self.parent.leader:
            logger.debug(f"P {self.process_id}: I am the leader at the moment, no need "
                         f"to call an election")
            return

        logger.info(f"P {self.process_id}: Calling for election")

        self.job_call.pause()
        self.job_call_response.pause()
        self.election_was_answered = False

        process_ids = self.parent.vector.index.keys()
        higher_processes = [x for x in process_ids if self.process_id < x]
        for process_id in higher_processes:
            self.parent.send(messages.ELECTION, None, process_id)

        self.resume_job(self.job_call_response, self.RESPONSE_TIMEOUT)

    def _check_election_responses(self):
        self.job_call_response.pause()

        if self.election_was_answered is True:
            logger.debug(
                f"P {self.process_id}: Another process answered, they will be the leader")
            return

        logger.info(f"P {self.process_id}: ANNOUNCING LEADERSHIP")
        self.parent.leader = True
        self.parent.send(messages.COORDINATOR, None, None)

    def handle_election(self, package):
        if not package.message_type == messages.ELECTION:
            logger.error(
                f"P {self.process_id}: Asked to handle wrong message type "
                f"{package.message_type}")
            return

        logger.debug(
            f"P {self.process_id}: Received election message from "
            f"{package.vector.process_id}")

        if package.vector.process_id < self.process_id:
            # My process ID is higher, so respond
            self.parent.send(messages.VOTE, self.process_id, package.vector.process_id)

    def handle_vote(self, package):
        if not package.message_type == messages.VOTE:
            logger.error(
                f"P {self.process_id}: Asked to handle wrong message type "
                f"{package.message_type}")
            return

        if package.vector.process_id < self.process_id:
            logger.info(
                f"P {self.process_id}: Received {messages.VOTE} message from lower "
                f"process id {package.vector.process_id}")
            self.call_for_election()
            return

        logger.warning(
            f"P {self.process_id}: Received vote message from "
            f"{package.vector.process_id}")
        if self.election_was_answered is False:
            self.election_was_answered = True

            self.resume_job(self.job_coordinator, self.COORDINATE_TIMEOUT)

    def _check_coordinator_response(self):
        self.job_coordinator.pause()

        if self.last_coordinator_msg is None or self.last_coordinator_msg < time.time() \
                - self.COORDINATE_TIMEOUT:
            logger.info(
                f"P {self.process_id}: Received no coordinate message from new elected "
                f"leader. Did it crash?")
            self.call_for_election()

    def handle_coordinate(self, package):
        if not package.message_type == messages.COORDINATOR:
            logger.error(
                f"P {self.process_id}: Asked to handle wrong message type "
                f"{package.message_type}")
            return

        is_leader = (self.process_id == package.vector.process_id)
        self.parent.leader = is_leader

        self.job_call_response.pause()
        self.job_coordinator.pause()

        self.last_coordinator_msg = time.time()

        if package.vector.process_id < self.process_id:
            logger.warn(
                f"P {self.process_id}: Received {messages.COORDINATOR} message from "
                f"lower process id {package.vector.process_id}")
            self.call_for_election()
            return

        logger.info(f"P {self.process_id}: received coordinate message from "
                    f"{package.vector.process_id}. Am I a leader? {is_leader}")

        # Start elections again in the future
        self.resume_job(self.job_call, self.CALL_TIMEOUT)

    @property
    def process_id(self):
        return self.parent.vector.process_id

    def resume_job(self, job, interval):
        # Let job start again with a the full interval to go.
        job.reschedule('interval', minutes=interval / 60)
        job.resume()
