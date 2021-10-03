from abc import ABC, abstractmethod
from collections import deque


class AbstractTree(ABC):
    @property
    @abstractmethod
    def children(self):
        """
        :return:
        Iterable object with child nodes
        """
        raise NotImplementedError

    def children_first_order_traverse_iterator(self, children_attribute=None):
        """
        Generator. Traverse through all child tree nodes then goes to parent node
        :param children_attribute: attribute name or method name which returns iterable object with child nodes.
        """
        children_attribute = children_attribute or 'children'

        first_stack = deque()
        second_stack = deque()

        first_stack.append(self)

        while first_stack:
            tree = first_stack.pop()
            second_stack.append(tree)

            children = getattr(tree, children_attribute)
            if callable(children):
                children = children()

            first_stack.extend(children)

        for tree in reversed(second_stack):
            yield tree

    def parent_first_order_traverse_iterator(self, children_attribute=None):
        """
        Generator. Traverse through parent node then goes to children
        :param children_attribute: attribute name or method name which returns iterable object with child nodes
        """
        children_attribute = children_attribute or 'children'
        stack = deque()
        stack.append(self)
        while stack:
            tree = stack.pop()
            yield tree

            children = getattr(tree, children_attribute)
            if callable(children):
                children = children()

            stack.extend(reversed(list(children)))
