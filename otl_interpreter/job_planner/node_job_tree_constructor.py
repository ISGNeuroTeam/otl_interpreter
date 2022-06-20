import datetime

from .node_job_tree import NodeJobTree
from .command_tree import CommandTree
from .result_address import ResultAddress

from otl_interpreter.interpreter_db.enums import ResultStorage, ComputingNodeType
from otl_interpreter.interpreter_db import node_commands_manager

from .sys_commands import SysReadInterprocCommand, SysWriteInterprocCommand, SysReadWriteCommand


class NodeJobTreeStorage:
    """
    Storage for mapping command tree to node job
    """
    def __init__(self):
        self._node_job_tree_for_command_tree = {}

        # keep nodejob for address object
        self._node_job_tree_for_address = {}

    def set_node_job_tree_for_command_tree(self, command_tree, node_job_tree):
        self._node_job_tree_for_command_tree[id(command_tree)] = node_job_tree

    def get_node_job_tree_for_command_tree(self, command_tree):
        return self._node_job_tree_for_command_tree[id(command_tree)]


def make_node_job_tree(top_command_tree, tws=None, twf=None, shared_post_processing=True, subsearch_is_node_job=False):
    """
    :param top_command_tree: root command tree with defined computing node type
    :param tws: time window start
    :param twf: time window finish
    :param shared_post_processing: for post processing, define result storage type (shared or local)
    :param subsearch_is_node_job: flag, make node job for each subsearch
    """
    top_node_job = _construct_node_job_tree(top_command_tree, subsearch_is_node_job)

    # top node job don't have result_address object yet
    # top node result address creation is individual case
    _make_address_for_result_node_job(top_node_job, shared_post_processing)

    # set time window for commands
    # set timestamp to not idempotent commands
    _set_arguments_to_commands(top_node_job, tws, twf)

    # calculate result dataframe paths as hash of command tree json
    _set_read_write_commands_paths(top_node_job)

    # put subsearches to command arguments
    _put_subsearches_to_command_arguments(top_node_job)

    return top_node_job


def _construct_node_job_tree(top_command_tree, subsearch_is_node_job=False):
    """
    Group command trees in node jobs trees by computing_node_type
    :param top_command_tree:
    :param shared: for post processing, define result storage
    :return:
    NodeJobTree
    """
    top_node_job = NodeJobTree(top_command_tree)

    node_job_tree_storage = NodeJobTreeStorage()

    node_job_tree_storage.set_node_job_tree_for_command_tree(
        top_command_tree, top_node_job
    )

    for command_tree in top_command_tree.parent_first_order_traverse_iterator():
        nodejob_for_subsearch_was_created = _define_node_job_tree_for_subsearch_command_trees(
            command_tree,
            node_job_tree_storage,
            subsearch_is_node_job
        )
        _define_node_job_tree_for_previous_command_tree_in_pipeline(
            command_tree, node_job_tree_storage, nodejob_for_subsearch_was_created
        )

        _define_node_job_tree_for_awaited_command_trees(command_tree, node_job_tree_storage)

    return top_node_job


def _set_read_write_commands_paths(top_node_job):
    # set paths for all node jobs addresses
    for node_job in top_node_job.children_first_order_traverse_iterator():
        # calculate command tree json before path set
        node_job.make_command_tree_json()
        if isinstance(node_job.command_tree.command, SysReadWriteCommand):
            # make path as command tree json hash
            path = node_job.command_tree_hash
            node_job.set_path_for_result_address(path)


def _set_arguments_to_commands(top_node_job, tws, twf):
    """
    Sets additional named arguments for all commands with use_timewindow (earliest, latest) and idempotent flag
    """
    for node_job in top_node_job.parent_first_order_traverse_iterator():
        for command in node_job.command_iterator():
            command_attr = node_commands_manager.get_command_attributes(command.name)
            if command_attr['use_timewindow']:
                command.add_argument('earliest', value=int(tws.timestamp()), key='earliest')
                command.add_argument('latest', value=int(twf.timestamp()), key='latest')
            if not command_attr['idempotent']:
                command.add_argument(
                    '__timestamp__', value=int(datetime.datetime.now().timestamp()), key='__timestamp__'
                )

def _put_subsearches_to_command_arguments(top_node_job):
    """
    Put all subsearches in arguments of Command ojects
    """
    for node_job in top_node_job.children_first_order_traverse_iterator():
        node_job.set_subsearches_to_command_arguments()


def _make_address_for_result_node_job(top_node_job, shared_post_processing):
    result_storage_type = None
    if top_node_job.computing_node_type == ComputingNodeType.POST_PROCESSING.value:
        if shared_post_processing:
            result_storage_type = ResultStorage.SHARED_POST_PROCESSING.value
        else:
            result_storage_type = ResultStorage.LOCAL_POST_PROCESSING

    else:
        result_storage_type = ResultStorage.INTERPROCESSING.value

    result_address = ResultAddress(top_node_job, result_storage_type)
    return result_address


def _create_new_node_job_for_child_command_tree(command_tree, child_command_tree, node_job_tree_storage):
    """
    Split commandre in two parts, creates node_job_tree for child_command_tree
    command_tree gets sys_read_interproc command as a child command
    child_command_tree gets sys_write_interproc command on the top

    :param command_tree: command_tree
    :param child_command_tree: child command tree of command_tree
    :param node_job_tree_storage: storage for node_jobs
    command_tree alreage has created node_job
    :return:
    read_sys_command_tree, invoke function will attach it to childs tree
    """
    node_job_tree = node_job_tree_storage.get_node_job_tree_for_command_tree(command_tree)

    # create write and read interproc commands
    write_sys_command = SysWriteInterprocCommand()
    read_sys_command = SysReadInterprocCommand()

    read_sys_command_tree = CommandTree(read_sys_command)
    write_sys_command_tree = CommandTree(write_sys_command)

    write_sys_command_tree.set_computing_node_type(child_command_tree.computing_node_type)
    read_sys_command_tree.set_computing_node_type(command_tree.computing_node_type)

    # put write command on the top of child command tree and create new node job
    write_sys_command_tree.set_previous_command_tree_in_pipeline(child_command_tree)
    new_node_job_tree = NodeJobTree(write_sys_command_tree, parent_node_job_tree=node_job_tree)

    # child command tree creates dataframe for command tree
    # create address object for result, path to result will be generated later
    # so save address to node storage

    result_address = ResultAddress(new_node_job_tree)

    # add read_sys_command to address object, path argument will be initialized after
    # write_sys_command has been added already in init
    result_address.add_command(read_sys_command)

    # save relations between command trees and node jobs
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


def _define_node_job_tree_for_previous_command_tree_in_pipeline(
        command_tree, node_job_tree_storage, nodejob_for_subsearch_was_created=False
):
    """
    Create node job for previous command tree in pipeline
    :param command_tree: command tree
    :param node_job_tree_storage: storage for relations between node job and command trees
    :param nodejob_for_subsearch_was_created: if true then node job need to be created anyway
    """
    if command_tree.previous_command_tree_in_pipeline is None:
        return

    node_job_tree = node_job_tree_storage.get_node_job_tree_for_command_tree(command_tree)

    if nodejob_for_subsearch_was_created or \
            command_tree.previous_command_tree_in_pipeline.computing_node_type != command_tree.computing_node_type or \
            command_tree.previous_command_tree_in_pipeline.always_new_node_job:

        read_sys_command_tree = _create_new_node_job_for_child_command_tree(
            command_tree, command_tree.previous_command_tree_in_pipeline, node_job_tree_storage,
        )

        # add read_sys_command to current command tree
        # write_sys_command has been added to previous command tree in _create_new_node_job_for_child_command_tree
        command_tree.set_previous_command_tree_in_pipeline(read_sys_command_tree)

    else:
        node_job_tree_storage.set_node_job_tree_for_command_tree(
            command_tree.previous_command_tree_in_pipeline, node_job_tree
        )


def _define_node_job_tree_for_subsearch_command_trees(command_tree, node_job_tree_storage, subsearch_is_node_job):
    """
    Creates child node jobs for subsearches if condition is true
    :param command_tree: command tree
    :param node_job_tree_storage: storage for relations between node job and command trees
    :param subsearch_is_node_job: if True then create subsearch anyway, else check computing node type
    Returns True if node job was created
    """
    node_job_was_created = False

    node_job_tree = node_job_tree_storage.get_node_job_tree_for_command_tree(command_tree)

    new_command_trees_for_subsearches = {}

    for index, subsearch_command_tree in enumerate(command_tree.subsearch_command_trees):

        if subsearch_is_node_job or (subsearch_command_tree.computing_node_type != command_tree.computing_node_type):
            read_sys_command_tree = _create_new_node_job_for_child_command_tree(
                command_tree, subsearch_command_tree, node_job_tree_storage
            )
            new_command_trees_for_subsearches[index] = read_sys_command_tree
            node_job_was_created = True

        else:
            node_job_tree_storage.set_node_job_tree_for_command_tree(
                subsearch_command_tree,
                node_job_tree
            )

    # replace old command trees with new command trees
    for index, new_subsearch_command_tree in new_command_trees_for_subsearches.items():
        command_tree.replace_subsearch_command_tree(new_subsearch_command_tree, index)

    return node_job_was_created


def _define_node_job_tree_for_awaited_command_trees(command_tree, node_job_tree_storage):

    node_job_tree = node_job_tree_storage.get_node_job_tree_for_command_tree(command_tree)

    for awaited_command_tree in command_tree.awaited_command_trees:
        awaited_node_job = _construct_node_job_tree(awaited_command_tree)

        # add sys_write_ineterproc command for saving result
        command_tree = awaited_node_job.command_tree
        sys_write_inter_proc_command = SysWriteInterprocCommand()

        command_tree_with_write_inter_proc = CommandTree(
            sys_write_inter_proc_command,
            previous_command_tree_in_pipeline=command_tree,
        )
        awaited_node_job.command_tree = command_tree_with_write_inter_proc
        awaited_node_job.set_parent_node_job_tree(node_job_tree)
        result_address = ResultAddress(awaited_node_job)

