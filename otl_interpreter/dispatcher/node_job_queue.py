import json

from typing import Union, Dict, Tuple
from uuid import UUID

from core.settings import REDIS_CONFIG

from otl_interpreter.settings import ini_config
from otl_interpreter.utils.priority_queue import RedisPriorityQueue, PriorityQueue


class DictQueue(dict):
    """
    This class the same as python defaultdict but creates only PriorityQueue or RedisPriorityQueue
    Passes dictionary key to Queue initializer
    """
    def __init__(self, one_process_mode=False):
        self._one_process_mode = one_process_mode
        super().__init__()

    def __missing__(self, key):
        self[key] = self._create_queue_for_computing_node_type(key)
        return self[key]

    def _create_queue_for_computing_node_type(
            self, computing_node: str
    ) -> Union[RedisPriorityQueue, PriorityQueue]:
        if self._one_process_mode:
            return PriorityQueue()
        else:
            return RedisPriorityQueue(computing_node, REDIS_CONFIG)


class NodeJobQueue:
    def __init__(self, one_process_mode=False):

        self.queues: Dict[str, Union[RedisPriorityQueue, PriorityQueue]]

        self.queues = DictQueue(one_process_mode)

    def add(self, node_job_dict: dict, priority_score: float) -> None:
        """
        Adds node job represented as dictionary to corresponding priority queue with score
        :param node_job_dict: node job as dictionary
        :param priority_score: any float as priority
        :return:
        """
        # uuid serialization
        node_job_dict = node_job_dict.copy()
        node_job_dict['uuid'] = node_job_dict['uuid'].hex
        self.queues[node_job_dict['computing_node_type']].add(priority_score, json.dumps(node_job_dict).encode())

    def pop(self, computing_node_type: str) -> Tuple[dict, float]:
        """
        Returns tuple node job dict with lowest score and it's score
        """
        node_job_dict_bin, priority = self.queues[computing_node_type].pop(count=1, min_score=True)[0]

        # uuid deserialization
        node_job_dict = json.loads(node_job_dict_bin)
        node_job_dict['uuid'] = UUID(node_job_dict['uuid'])

        return node_job_dict, priority

    def node_jobs_in_queue(self, computing_node_type: str):
        """
        Returns number of node jobs in queue for computing node type
        """
        return len(self.queues[computing_node_type])

    def computing_node_types(self):
        """
        Returns iterable ( dict keys ) of computing node types
        """
        return self.queues.keys()


dispatcher_config = ini_config['dispatcher']

node_job_queue = NodeJobQueue(
    one_process_mode=(dispatcher_config['one_process_mode'].lower() == 'true')
)

