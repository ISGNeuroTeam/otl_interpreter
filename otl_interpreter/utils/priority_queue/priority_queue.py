from collections import defaultdict

from typing import List, Tuple, Union

from .abstract_priority_queue import AbstractPriorityQueue


class PriorityQueue(AbstractPriorityQueue):
    def __init__(self) -> None:
        # set of elements for every score
        self._score_dict = defaultdict(set)

        # score for every element
        self._elements = dict()

    def __len__(self):
        return len(self._elements)

    @staticmethod
    def _to_bytes(value):
        if isinstance(value, str):
            return value.encode()
        if isinstance(value, bytes):
            return value
        raise ValueError("Prioriry queue except only 'bytes' and 'str'")

    def add(self, score: float, *elements: Union[str, bytes]) -> None:
        """
        Push in queue elements with score
        :param score: score
        :param elements: elements to push to queue
        :return:
        """
        score = float(score)
        elements = [self._to_bytes(element) for element in elements]
        self._score_dict[score].update(elements)

        self._elements.update(
            {element: score for element in elements}
        )

    def pop(self, count: int = 1, min_score=False) -> List[Tuple[bytes, float]]:
        """
        Returns <count> elements from queue with highest score as list of tuples ( binary, score )
        if min_score is True returns <count> elements from queue with lowest score
        """
        if count < 1 or len(self._elements) < 1:
            return []

        sorted_score_list = sorted(self._score_dict.keys(), reverse=not min_score)

        # find score keys  set that contain >= count  elements
        # find index in score list
        index_in_score_list = 0
        elements_counter = 0
        for score in sorted_score_list:
            elements_counter += len(self._score_dict[score])
            index_in_score_list += 1
            if elements_counter >= count:
                break

        result_elements = list()

        # add in result elements all elements with score  that have index in sorted_score_list < index_in_score_list
        for index in range(index_in_score_list-1):
            score = sorted_score_list[index]
            for element in sorted(self._score_dict[score], reverse=not min_score):
                result_elements.append(
                    (element, score, )
                )
            del self._score_dict[score]

        # get shortage of elements from last score
        score = sorted_score_list[index_in_score_list-1]
        for element in sorted(self._score_dict[score], reverse=not min_score):
            if len(result_elements) == count:
                break
            result_elements.append(
                (element, score, )
            )
            self._score_dict[score].discard(element)

            if len(self._score_dict[score]) == 0:
                del self._score_dict[score]

        # remove returned elements
        for element, score in result_elements:
            self._elements.pop(element)

        return result_elements








