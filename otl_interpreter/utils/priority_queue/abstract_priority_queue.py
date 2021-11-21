from abc import ABC, abstractmethod

from typing import List, Tuple, Union


class AbstractPriorityQueue(object):
    @abstractmethod
    def add(self, score: float, *elements: Union[str, bytes]) -> None:
        """
        Push in queue elements with score
        :param score: score
        :param elements: elements to push to queue
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def pop(self, count: int = 1, min_score=False) -> List[Tuple[bytes, float]]:
        """
        Returns <count> elements from queue with highest score
        if min_score is True returns <count> elements from queue with lowest score
        """
        raise NotImplementedError




