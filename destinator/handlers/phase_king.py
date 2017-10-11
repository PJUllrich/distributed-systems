from collections import Counter
import json
import logging


import destinator.const.messages as messages

logger = logging.getLogger(__name__)


class PhaseKing:
    """
    The PhaseKing algorithm allows to agree on a decision, although a traitor might be
    in the system. The PhaseKing algorithm allows for less than 1/4 traitors/byzantine
    processes. So, at least 5 processes are required (then 1 process might be byzantine)
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
            self.job_call.resume()
            self.is_running = True

    def stop(self):
        logger.info(f"P {self._process_id}:  Stop PhaseKing (byzantine: "
                    f"{self._is_byzantine})")
        self.job_call.pause()
        self.is_running = False

    def _init_jobs(self):
        """
        Initiate the used jobs
        """
        scheduler = self.parent.scheduler

        self.job_call = scheduler.add_job(
            self.init_new_round, 'interval', minutes=self.INIT_SCHEDULE / 60,
            replace_existing=True, id=self.PHASE_KING_INIT_JOB_ID)
        self.job_call.pause()

        self.job_start = scheduler.add_job(
            self.decide_first_round, 'interval', minutes=self.START_TIMEOUT / 60,
            replace_existing=True, id=self.PHASE_KING_START_JOB_ID)
        self.job_start.pause()

    def init_new_round(self):
        if not self.parent.is_leader:
            logger.warning(f"P {self._process_id}: Called, although I am not a leader")
            return

        logger.info(f"P {self._process_id}: Starting new PhaseKing run")
        self.job_call.pause()

        self._init_new_run()

        self.parent.send(messages.PHASE_KING_INIT, "")
        self.job_start.resume()

    def _init_new_run(self):
        self.majorities = [self._value]
        self.participants = [self._process_id]
        self.received = [self._value]

    def handle_init(self, package):
        if not package.message_type == messages.PHASE_KING_INIT:
            logger.debug(f"Received message, but wrong handler {package.message_type}")
            return

        # New round of phase king is about to start
        self._init_new_run()

        # Add process id of other device
        self.participants = sorted(self.participants + [package.vector.process_id])
        logger.info(f"P {self._process_id}:  The following participants are active: "
                    f"{self.participants}")

        payload = self._pack_payload(0, self._value)
        self.parent.send(messages.PHASE_KING_SEND, payload)

        self.job_start.resume()

    def handle_send(self, package):
        if not package.message_type == messages.PHASE_KING_SEND:
            logger.debug(f"Received message, but wrong handler {package.message_type}")
            return

        round, value = self._unpack_payload(package.payload)
        if round is 0:
            # Add process id of other device
            self.participants = sorted(self.participants + [package.vector.process_id])
            logger.info(f"P {self._process_id}: The following participants are active: "
                        f"{self.participants}")
        if round != 0 or self.parent.is_leader:
            self.received.append(value)

        if not self.parent.is_leader and len(self.received) == len(self.participants):
            # the leader has to find first all participants and therefore uses a timeout
            self.execute_decision(self.received, round)

    def decide_first_round(self):
        self.job_start.pause()

        if len(self.participants) < 5:
            logger.info(f"P {self._process_id}: Not enough participants "
                        f"{len(self.participants)}")
            self.job_call.resume()
            return

        self.execute_decision(self.received, 0)

    def execute_decision(self, received_values, current_round):
        majority = self._get_majority(received_values)

        payload = self._pack_payload(current_round, majority)
        self.parent.send(messages.PHASE_KING_DECISION, payload)
        self.majorities.append(majority)

    def handle_decision(self, package):
        round, majority = self._unpack_payload(package.payload)
        self.majorities.append(majority)

        if len(self.majorities) > len(self.participants) / 4 + 1:
            logger.info(f"P {self._process_id}: Got majority of: "
                        f"{self._get_majority(self.majorities)} out of "
                        f"{len(self.majorities) - 1} rounds")
            return

        self.received = [self._value]

        new_round = round + 1
        logger.debug(f"P {self._process_id}: Sending data for new round {new_round}"
                     f"Participants: {self.participants}")
        decider = self.participants[new_round]

        payload = self._pack_payload(new_round, self._value)
        self.parent.send(messages.PHASE_KING_SEND, payload, decider)

    @classmethod
    def _get_majority(cls, items):
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
        return self.parent.is_leader

    @property
    def _value(self):
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
