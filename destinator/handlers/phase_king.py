import json
import logging
from collections import Counter

import destinator.const.messages as messages

logger = logging.getLogger(__name__)


class PhaseKing:
    """
    The PhaseKing algorithm allows to agree on a decision, although a traitor might be
    in the system. The PhaseKing algorithm allows for less than 1/4 traitors/byzantine
    processes. So, at least 5 processes are required (then 1 process might be byzantine)

    Currently, the leader acts always byzantine. It is assumed that the leader does not
    change during one execution
    1. Phase discovery of all active devices
    2. Starting of the round based majority voting
    """
    PHASE_KING_INIT_JOB_ID = "PHASE_KING_JOB_INIT"
    INIT_SCHEDULE = 30
    PHASE_KING_START_JOB_ID = "PHASE_KING_JOB_START"
    START_TIMEOUT = 30

    FIELD_ROUND = "ROUND"
    FIELD_VALUE = "VALUE"

    VALUE_BYZANTINE = 66
    VALUE_CORRECT = 42

    def __init__(self, parent_handler):
        self.parent = parent_handler

        self.is_running = None

        self.participants = []
        self.majorities = []
        self.received = []

        self._init_jobs()

    def start(self):
        if self.is_running is not True:
            logger.info(f"P {self._process_id}: Resuming PhaseKing (byzantine: "
                        f"{self._is_byzantine})")
            self.resume_job(self.job_init, self.INIT_SCHEDULE)
            self.is_running = True

    def stop(self):
        logger.info(f"P {self._process_id}:  Stop PhaseKing (byzantine: "
                    f"{self._is_byzantine})")
        self.job_init.pause()
        self.job_start.pause()
        self.is_running = False

    def _init_jobs(self):
        """
        Initiate the used jobs
        """
        scheduler = self.parent.scheduler

        self.job_init = scheduler.add_job(
            self.init_new_round, 'interval', minutes=self.INIT_SCHEDULE / 60,
            replace_existing=True, id=self.PHASE_KING_INIT_JOB_ID)
        self.job_init.pause()

        self.job_start = scheduler.add_job(
            self.start_first_round, 'interval', minutes=self.START_TIMEOUT / 60,
            replace_existing=True, id=self.PHASE_KING_START_JOB_ID)
        self.job_start.pause()

    def init_new_round(self):
        """
        Leader initiates the algorithms and pings everyone
        """
        if not self.parent.is_leader:
            logger.warning(f"P {self._process_id}: Called, although I am not a leader")
            return

        logger.info(f"P {self._process_id}: Starting new PhaseKing run")
        self.job_init.pause()

        self._init_new_run()

        self.parent.send(messages.PHASE_KING_INIT, "")
        self.resume_job(self.job_start, self.START_TIMEOUT)

    def handle_init(self, package):
        """
        Devices is active and responds to ping
        """
        if not package.message_type == messages.PHASE_KING_INIT:
            logger.debug(f"Received message, but wrong handler {package.message_type}")
            return

        # New round of phase king is about to start
        self._init_new_run()

        # Add process id of other device
        self.participants = sorted(self.participants + [package.vector.process_id])
        logger.info(f"P {self._process_id}: The following participants are active: "
                    f"{self.participants}")

        self.parent.send(messages.PHASE_KING_FOUND, self._value)
        self.resume_job(self.job_start, self.START_TIMEOUT)

    def _init_new_run(self):
        self.majorities = [self._value]
        self.participants = [self._process_id]
        self.received = [self._value]

    def handle_found(self, package):
        """
        Got message from other active device
        """
        if not package.message_type == messages.PHASE_KING_FOUND:
            logger.debug(f"Received message, but wrong handler {package.message_type}")
            return

        # Add process id of other device
        self.participants = sorted(self.participants + [package.vector.process_id])
        logger.info(f"P {self._process_id}: The following participants are active: "
                    f"{self.participants}")
        self.received.append(package.payload)

    def start_first_round(self):
        """
        Start first round of Phase King, if I am the lowest process of all participants
        """
        self.job_start.pause()

        if len(self.participants) < 5:
            logger.info(f"P {self._process_id}: Not enough participants "
                        f"{len(self.participants)}")
            # Initiate a new run
            if self.parent.is_leader:
                self.resume_job(self.job_init, self.INIT_SCHEDULE)
            return

        if self._process_id == self.participants[0]:
            self.execute_decision(self.received, 0)

    def handle_send(self, package):
        """
        I am the King process this round and received messages from the other processes
        After getting all messages, a decision needs to be made
        """
        if not package.message_type == messages.PHASE_KING_SEND:
            logger.debug(f"Received message, but wrong handler {package.message_type}")
            return

        round, value = self._unpack_payload(package.payload)

        # Received a valid broadcast value from another process
        self.received.append(value)

        if len(self.received) == len(self.participants):
            self.execute_decision(self.received, round)

    def execute_decision(self, received_values, current_round):
        """
        Make a decision on majority and inform everyone
        """
        majority = self._get_majority(received_values)

        payload = self._pack_payload(current_round, majority)
        self.parent.send(messages.PHASE_KING_DECISION, payload)

        if self._process_id == self.participants[current_round]:
            # Handle own decision message
            self.handle_decision_msg(current_round, majority)

    def handle_decision(self, package):
        """
        Got a decision message, handle it
        """
        if not package.message_type == messages.PHASE_KING_DECISION:
            logger.debug(f"Received message, but wrong handler {package.message_type}")
            return

        round, majority = self._unpack_payload(package.payload)
        if round == 0:
            # In the first round, all received values (although not necessarily for us
            # indented package are saved. -> RESET
            self.received = [self._value]
        self.handle_decision_msg(round, majority)

    def handle_decision_msg(self, round, majority):
        """
        Got a decision, save it
        Start the next round, if not done
        """
        self.majorities.append(majority)

        if len(self.majorities) > len(self.participants) / 4 + 1:
            # The algorithm is done
            logger.info(f"P {self._process_id}: Got majority of: "
                        f"{self._get_majority(self.majorities)} out of "
                        f"{len(self.majorities) - 1} rounds [values: {self.majorities}]")

            # Initiate a new run
            if self.parent.is_leader:
                self.resume_job(self.job_init, self.INIT_SCHEDULE)
            return

        new_round = round + 1
        logger.debug(f"P {self._process_id}: Sending data for new round {new_round}. "
                     f"Participants: {self.participants}")
        decider = self.participants[new_round]

        payload = self._pack_payload(new_round, self._value)
        self.parent.send(messages.PHASE_KING_SEND, payload, decider)

    @classmethod
    def _get_majority(cls, items):
        """
        Get the most frequent item of a list
        Parameters
        ----------
        items: list of items

        Returns
        -------
        The most frequent item
        """
        count = Counter(items)
        majority_item = count.most_common(1)[0]
        majority = majority_item[0]
        return majority

    @classmethod
    def _pack_payload(cls, round, value):
        data = {
            cls.FIELD_ROUND: round,
            cls.FIELD_VALUE: value,
        }
        return json.dumps(data)

    @classmethod
    def _unpack_payload(cls, payload):
        data = json.loads(payload)
        round = data.get(cls.FIELD_ROUND)
        value = data.get(cls.FIELD_VALUE)
        return round, value

    @property
    def _is_byzantine(self):
        """
        Only the leader process is byzantine
        """
        return self.parent.is_leader

    @property
    def _value(self):
        """
        The decided on value to gain a majority on
        """
        if self._is_byzantine:
            return self.VALUE_BYZANTINE
        return self.VALUE_CORRECT

    @property
    def _process_id(self):
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
