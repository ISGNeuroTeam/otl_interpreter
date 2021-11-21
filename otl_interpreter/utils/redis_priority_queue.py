import redis

from typing import List, Tuple, Union


class RedisPriorityQueue(object):
    def __init__(self, queue_name: str, redis_config: dict) -> None:
        self.queue_name = queue_name
        self._r = redis.Redis(
            host=redis_config['host'],
            port=redis_config['port'],
            db=redis_config['db'],
            password=redis_config['password']
        )

    def __del__(self):
        self._r.delete(self.queue_name)

    def push(self, score: float, *elements: Union[str, bytes]) -> None:
        """
        Push in queue elements with score
        :param score: score
        :param element: element to push to queue
        :return:
        """
        mapping = {element: score for element in elements}

        self._r.zadd(self.queue_name, mapping)

    def pop(self, count: int = 1, min_score=False) -> List[Tuple[bytes, float]]:
        """
        Returns <count> elements from queue with highest score
        if min_score is True returns <count> elements from queue with lowest score
        """
        if min_score:
            return self._r.zpopmin(self.queue_name, count)
        else:
            return self._r.zpopmax(self.queue_name, count)




