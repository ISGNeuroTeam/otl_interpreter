import hashlib
import json
import uuid

from .abstract_tree import AbstractTree


class NodeJobTree(AbstractTree):
    """
    Tree. Node contains command_tree without awaited child command trees
    All awaited child commands goes to child node for each awaited command awaited_node_job_tree

    """

    def __init__(self, command_tree, parent_node_job_tree=None):
        """
        :param command_tree: command_tree
        :param tws: time window start
        :param twf: time window finish
        :param parent_node_job_tree:
        """
        self.command_tree = command_tree

        self.parent_node_job_tree = None
        self.awaited_node_job_trees = []
        self.computing_node_type = command_tree.computing_node_type

        if parent_node_job_tree is not None:
            self.set_parent_node_job_tree(parent_node_job_tree)

        self.uuid = uuid.uuid4()
        self.result_address = None

        self._command_tree_hash = None
        self._command_tree_json = None

        self.cache_ttl = None

        # flag for indicating cache existence
        self.result_calculated = False

    @property
    def children(self):
        return self.awaited_node_job_trees

    @property
    def parent(self):
        return self.parent_node_job_tree

    @property
    def command_tree_hash(self):
        if self._command_tree_hash is None:
            self.calculate_command_tree_hash()
        return self._command_tree_hash

    @property
    def command_tree_json(self):
        """
        Returns command tree as json. Command tree may change. Invokes make_command_tree_json to rebuild json.
        """
        if self._command_tree_json is None:
            return self.make_command_tree_json()
        return self._command_tree_json

    def calculate_command_tree_hash(self):
        """
        calculates hash for command tree
        :return:
        """
        command_tree_json = self.make_command_tree_json()

        # command_tree + time window -> hash
        command_tree_hash = hashlib.blake2b(
            command_tree_json.encode()
        )
        self._command_tree_hash = command_tree_hash.digest().hex()
        return self._command_tree_hash

    def make_command_tree_json(self):
        command_tree_list = self.as_command_dict_list()
        self._command_tree_json = json.dumps(command_tree_list)
        return self._command_tree_json

    def set_parent_node_job_tree(self, parent_node_job_tree):
        self.parent_node_job_tree = parent_node_job_tree
        if self not in self.parent_node_job_tree.awaited_node_job_trees:
            self.parent_node_job_tree.awaited_node_job_trees.append(self)

    def add_awaited_node_job_tree(self, node_job_tree):
        self.awaited_node_job_trees.append(node_job_tree)
        node_job_tree.parent_node_job_tree = self

    def as_command_dict_list(self):
        return [command_tree.as_command_dict() for command_tree in self.command_tree.through_pipeline_iterator()]

    def set_result_address(self, address):
        self.result_address = address

    def set_path_for_result_address(self, path):
        assert self.result_address is not None
        self.result_address.set_path(path)

    def command_iterator(self):
        """
        Iterates through all commands in node job
        :return:
        """
        for command_tree  in self.command_tree.parent_first_order_traverse_iterator('all_child_trees'):
            yield command_tree.command

    def set_subsearches_to_command_arguments(self):
        """
        Put subsearches in command argument for all commands in node job
        """
        for command_tree in self.command_tree.children_first_order_traverse_iterator():
            command_tree.set_subsearches_to_command_arguments()

    def get_top_command(self):
        """
        Returns top command instance in node job tree
        """
        return self.command_tree.command
