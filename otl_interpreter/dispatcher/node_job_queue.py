from typing import Union, Dict, Tuple
from core.settings import REDIS_CONFIG

from otl_interpreter.settings import ini_config
from otl_interpreter.interpreter_db.enums import ComputingNodeType
from otl_interpreter.utils.priority_queue import RedisPriorityQueue, PriorityQueue


class NodeJobQueue:
    def __init__(self, standalone=False):

        self.queues: Dict[str, Union[RedisPriorityQueue, PriorityQueue]]

        self.queues = {
            computing_node_type: self._create_queue_for_computing_node(
                computing_node_type, standalone
            )
            for computing_node_type in ComputingNodeType.values
        }

    @staticmethod
    def _create_queue_for_computing_node(
            computing_node: str, standalone: bool
    ) -> Union[RedisPriorityQueue, PriorityQueue]:

        if standalone:
            return PriorityQueue()
        else:
            return RedisPriorityQueue(computing_node, REDIS_CONFIG)

    def add(self, computing_node_type: str, node_job_guid: bytes, priority_score: float) -> None:
        """
        Adds node job guid to corresponding priority queue with score
        :param computing_node_type:
        :param node_job_guid:
        :param priority_score:
        :return:
        """
        self.queues[computing_node_type].add(priority_score, node_job_guid)

    def pop(self, computing_node_type: str, count: int = 1) -> Tuple[bytes, float]:
        """
        Returns tuple node job guid with lowest score and it's score
        """
        return self.queues[computing_node_type].pop(count=count, min_score=True)


dispatcher_config = ini_config['dispatcher']

node_job_queue = NodeJobQueue(
    standalone=(dispatcher_config['standalone'].lower() == 'true')
)

