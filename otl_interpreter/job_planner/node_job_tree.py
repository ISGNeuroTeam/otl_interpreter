from uuid import uuid4

from .abstract_tree import AbstractTree
from .command_tree import CommandTree
from .sys_commands import make_sys_read_interproc, make_sys_write_interproc


class NodeJobTree(AbstractTree):
    """
    Tree. Node contains command_tree without awaited child command trees
    All awaited child commands goes to child node for each awaited command awaited_node_job_tree

    """

    def __init__(self, command_tree, parent_node_job_tree=None):
        self.command_tree = command_tree
        self.parent_node_job_tree = None
        self.awaited_node_job_trees = []

        self.computing_node_type = command_tree.computing_node_type

        if parent_node_job_tree is not None:
            self.set_parent_node_job_tree(parent_node_job_tree)

    @property
    def children(self):
        return self.awaited_node_job_trees

    @property
    def parent(self):
        return self.parent_node_job_tree

    def set_parent_node_job_tree(self, parent_node_job_tree):
        self.parent_node_job_tree = parent_node_job_tree
        if self not in self.parent_node_job_tree.awaited_node_job_trees:
            self.parent_node_job_tree.awaited_node_job_trees.append(self)

    def add_awaited_node_job_tree(self, node_job_tree):
        self.awaited_node_job_trees.append(node_job_tree)
        node_job_tree.parent_node_job_tree = self


class NodeJobTreeStorage:
    def __init__(self):
        self._node_job_tree_for_command_tree = {}

    def set_node_job_tree_for_command_tree(self, command_tree, node_job_tree):
        self._node_job_tree_for_command_tree[id(command_tree)] = node_job_tree

    def get_node_job_tree_for_command_tree(self, command_tree):
        try:
            result =  self._node_job_tree_for_command_tree[id(command_tree)]
        except KeyError as err:
            print('!!!!!Not found node job for command')
            print(command_tree.command.name)
            raise err
        return result


def _make_address_for_interproc_dataframe():
    return uuid4().hex


def make_node_job_tree(top_command_tree):
    """
    Group command trees in node jobs trees by computing_node_type
    :param top_command_tree:
    :return:
    NodeJobTree
    """
    top_node_job = NodeJobTree(top_command_tree)
    node_job_tree_storage = NodeJobTreeStorage()

    node_job_tree_storage.set_node_job_tree_for_command_tree(
        top_command_tree, top_node_job
    )

    for command_tree in top_command_tree.parent_first_order_traverse_iterator():

        _define_node_job_tree_for_previous_command_tree_in_pipeline(command_tree, node_job_tree_storage)

        _define_node_job_tree_for_subsearch_command_trees(command_tree, node_job_tree_storage)

        _define_node_job_tree_for_awaited_command_trees(command_tree, node_job_tree_storage)

    return top_node_job


def _create_new_node_job_for_child_command_tree(command_tree, child_command_tree, node_job_tree_storage):
    node_job_tree = node_job_tree_storage.get_node_job_tree_for_command_tree(command_tree)

    dataframe_adderss = _make_address_for_interproc_dataframe()
    write_sys_command = make_sys_write_interproc(dataframe_adderss)
    read_sys_command = make_sys_read_interproc(dataframe_adderss)

    read_sys_command_tree = CommandTree(read_sys_command)
    write_sys_command_tree = CommandTree(write_sys_command)

    write_sys_command_tree.set_computing_node_type(child_command_tree.computing_node_type)
    read_sys_command_tree.set_computing_node_type(command_tree.computing_node_type)

    write_sys_command_tree.set_previous_command_tree_in_pipeline(child_command_tree)

    new_node_job_tree = NodeJobTree(write_sys_command_tree, parent_node_job_tree=node_job_tree)

    node_job_tree_storage.set_node_job_tree_for_command_tree(
        read_sys_command_tree,
        node_job_tree
    )

    node_job_tree_storage.set_node_job_tree_for_command_tree(
        write_sys_command_tree,
        new_node_job_tree
    )

    node_job_tree_storage.set_node_job_tree_for_command_tree(
        child_command_tree,
        new_node_job_tree
    )

    return read_sys_command_tree


def _define_node_job_tree_for_previous_command_tree_in_pipeline(command_tree, node_job_tree_storage):
    if command_tree.previous_command_tree_in_pipeline is None:
        return

    node_job_tree = node_job_tree_storage.get_node_job_tree_for_command_tree(command_tree)

    if command_tree.previous_command_tree_in_pipeline.computing_node_type != \
            command_tree.computing_node_type:

        read_sys_command_tree = _create_new_node_job_for_child_command_tree(
            command_tree, command_tree.previous_command_tree_in_pipeline, node_job_tree_storage
        )

        command_tree.set_previous_command_tree_in_pipeline(read_sys_command_tree)

    else:
        node_job_tree_storage.set_node_job_tree_for_command_tree(
            command_tree.previous_command_tree_in_pipeline, node_job_tree
        )


def _define_node_job_tree_for_subsearch_command_trees(command_tree, node_job_tree_storage):
    node_job_tree = node_job_tree_storage.get_node_job_tree_for_command_tree(command_tree)

    new_command_trees_for_subsearches = {}

    for index, subsearch_command_tree in enumerate(command_tree.subsearch_command_trees):

        if subsearch_command_tree.computing_node_type != command_tree.computing_node_type:
            read_sys_command_tree = _create_new_node_job_for_child_command_tree(
                command_tree, subsearch_command_tree, node_job_tree_storage
            )
            new_command_trees_for_subsearches[index] = read_sys_command_tree

        else:
            node_job_tree_storage.set_node_job_tree_for_command_tree(
                subsearch_command_tree,
                node_job_tree
            )

    for index, new_subsearch_command_tree in new_command_trees_for_subsearches.items():
        command_tree.replace_subsearch_command_tree(new_subsearch_command_tree, index)


def _define_node_job_tree_for_awaited_command_trees(command_tree, node_job_tree_storage):

    node_job_tree = node_job_tree_storage.get_node_job_tree_for_command_tree(command_tree)

    for awaited_command_tree in command_tree.awaited_command_trees:
        awaited_node_job = make_node_job_tree(awaited_command_tree)

        node_job_tree.add_awaited_node_job_tree(
            awaited_node_job
        )
