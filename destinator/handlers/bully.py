import logging
import time

import destinator.const.messages as messages

logger = logging.getLogger(__name__)


class Bully:
    """
    Implements the Bully algorithm which is used for leader election.
    It basically works in 3 phases:
     - Call for a new election
     - Voting on elections
     - Announce leadership (coordinate)
     To be able to check if processes have not responded to messages (within a
     timeout), task scheduling is used
    """

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

        self._init_jobs()

        # Start an election instantly
        self.call_for_election()

    def _init_jobs(self):
        """
        Initiate the used jobs
        """
        scheduler = self.parent.scheduler

        self.job_call = scheduler.add_job(
            self.call_for_election, 'interval', minutes=self.CALL_TIMEOUT / 60,
            replace_existing=True, id=self.BULLY_CALL_JOB_ID)
        self.job_call_response = scheduler.add_job(
            self._check_election_responses, 'interval',
            minutes=self.RESPONSE_TIMEOUT / 60, replace_existing=True,
            id=self.BULLY_RESPONSE_JOB_ID)
        self.job_call_response.pause()
        self.job_coordinator = scheduler.add_job(
            self._check_coordinator_response, 'interval',
            minutes=self.COORDINATE_TIMEOUT / 60, replace_existing=True,
            id=self.BULLY_COORDINATOR_JOB_ID)
        self.job_coordinator.pause()

    def call_for_election(self):
        """
        Start a new election process to determine the (new) leader
        """
        if self.process_id <= 0:
            logger.debug(f"P {self.process_id}: Process ID is not yet set")
            return
        if self.parent.is_leader:
            logger.info(f"P {self.process_id}: I am the leader at the moment, no need "
                        f"to call an election")
            return

        logger.info(f"P {self.process_id}: Calling for election")

        self.job_call.pause()
        self.job_call_response.pause()
        self.election_was_answered = False

        # Sending election message to all higher processes
        process_ids = self.parent.vector.index.keys()
        higher_process_ids = [x for x in process_ids if self.process_id < x]
        for process_id in higher_process_ids:
            self.parent.send(messages.ELECTION, None, process_id)

        self.resume_job(self.job_call_response, self.RESPONSE_TIMEOUT)

    def _check_election_responses(self):
        """
        Check whether all expected processes have responded, if not announce own
        leadership
        """
        self.job_call_response.pause()

        if self.election_was_answered is True:
            logger.debug(
                f"P {self.process_id}: Another process answered, they will be the leader")
            return

        logger.info(f"P {self.process_id}: ANNOUNCING LEADERSHIP")
        self.parent.set_leader(True)
        self.parent.send(messages.COORDINATOR, None)

    def handle_election(self, package):
        """
        Handle an election message

        Parameters
        ----------
        package: Package
            The received json package
        """
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
        """
        Handle a vote message

        Parameters
        ----------
        package: Package
            The received json package
        """
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
        """
        Check if after sending a vote message, also a coordinating (leader
        announcement) message was received
        """
        self.job_coordinator.pause()

        if self.last_coordinator_msg is None or self.last_coordinator_msg < time.time() \
                - self.COORDINATE_TIMEOUT:
            logger.info(
                f"P {self.process_id}: Received no coordinate message from new elected "
                f"leader. Did it crash?")
            self.call_for_election()

    def handle_coordinate(self, package):
        """
        Handle a coordinate message

        Parameters
        ----------
        package: Package
            The received json package
        """
        if not package.message_type == messages.COORDINATOR:
            logger.error(
                f"P {self.process_id}: Asked to handle wrong message type "
                f"{package.message_type}")
            return

        is_leader = (self.process_id == package.vector.process_id)
        self.parent.set_leader(is_leader)

        self.job_call_response.pause()
        self.job_coordinator.pause()

        self.last_coordinator_msg = time.time()

        if package.vector.process_id < self.process_id:
            logger.warning(
                f"P {self.process_id}: Received {messages.COORDINATOR} message from "
                f"lower process id {package.vector.process_id}")
            self.call_for_election()
            return

        logger.debug(f"P {self.process_id}: received coordinate message from "
                     f"{package.vector.process_id}. Am I a leader? {is_leader}")

        # Start elections again in the future
        self.resume_job(self.job_call, self.CALL_TIMEOUT)

    @property
    def process_id(self):
        """
        Gets the own process id

        Returns
        -------
        int
        """
        return self.parent.vector.process_id

    def resume_job(self, job, interval):
        """
        Let job start again with a the full interval to go.
        Parameters
        ----------
        job: Job
            The job to resume
        interval: int
            An interval in seconds
        """
        job.reschedule('interval', minutes=interval / 60)
        job.resume()
