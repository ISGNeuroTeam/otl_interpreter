from .exceptions import JobPlanException
from .abstract_tree import AbstractTree


INF = 2147483647


class WeightTree(AbstractTree):
    """
    Each WeightTree object corresponds to CommandTree
    """
    def __init__(self, command_tree, computing_node_types, parent_weight_tree=None):
        """
        :param computing_node_types: list of node types. Order matters. if two node types with same weight exist,
        first in list will be choosen
        :param command_tree: command tree
        """
        self.command_tree = command_tree

        self.parent_weight_tree = parent_weight_tree

        self.child_weight_trees = []

        self.computing_node_types = computing_node_types

        # minimum number of dataframe transfers if current command evaluates on node with that type
        self.weights = {computing_node_type: 0 for computing_node_type in computing_node_types}

    @property
    def children(self):
        return self.child_weight_trees

    @property
    def parent(self):
        return self.parent_weight_tree


def _derive_weight_from_child_weights(weight_tree, command_name_set):
    """
    Evaluate weights for weight tree.
    :param weight_tree:
    :param command_name_set: dictionary of  command name sets for every computing node type
    {computing_node_type: set(command_name1, command_name2....)}
    :return:
    """
    for computing_node_type in weight_tree.weights:

        if weight_tree.command_tree.command.name not in command_name_set[computing_node_type]:
            weight_tree.weights[computing_node_type] = INF

        else:
            weight_tree.weights[computing_node_type] = sum(
                _find_next_min_weight_for_node_type(computing_node_type, child_weight_tree.weights)
                for child_weight_tree in weight_tree.child_weight_trees
            )

    # if all weights are INF raise error
    if all(
        map(
            lambda value: value >= INF,
            weight_tree.weights.values()
        )
    ):
        raise JobPlanException(
            f'Can\'t define computnig node for {weight_tree.command_tree.command.name}  command'
        )


def _find_next_min_weight_for_node_type(next_node_type, cur_weights):
    """
    :param next_node_type: next computing node type
    :param cur_weights: {computing_node_type: weight}
    :return:
    min weight for next computing node type
    """
    # if node type differs from next_node_type  than make +1 dataframe transfer
    return min(
        cur_weights[computing_node_type] + int(computing_node_type != next_node_type)
        for computing_node_type in cur_weights
    )


def _define_computing_node_type(weight_tree, computing_node_type_priority_list):
    if weight_tree.parent_weight_tree:
        parent_computing_node_type = weight_tree.parent_weight_tree.command_tree.computing_node_type
    else:
        parent_computing_node_type = None

    computing_node_type = _find_computing_node_type_for_parent_node_type(
        parent_computing_node_type, weight_tree.weights, computing_node_type_priority_list
    )

    weight_tree.command_tree.set_computing_node_type(computing_node_type)


def _find_computing_node_type_for_parent_node_type(parent_node_type, cur_weights, computing_node_type_priority_list):
    """
    :param parent_node_type: parent command tree compuging node type
    :param cur_weights: weights of current weight tree
    :param computing_node_type_priority_list: list of computing node types in priority order
    :return:
    computing node type for current command tree with best weight
    """
    return min(
        computing_node_type_priority_list,
        key=lambda computing_node_type: cur_weights[computing_node_type] + int(computing_node_type != parent_node_type)
    )


def define_computing_node_type_for_command_tree(root_command_tree, computing_node_type_priority_list, command_name_set):
    """
    :param root_command_tree: CommandTree object
    :param computing_node_type_priority_list: computing node type names in priority order
    :param command_name_set: {computing_node_type: set(command_name1, command_name2....)}
    :return:
    """
    root_weight_tree = WeightTree(root_command_tree, computing_node_type_priority_list)

    weight_tree_for_command_tree = {root_command_tree: root_weight_tree}

    # construct weight trees
    for command_tree in root_command_tree.parent_first_order_traverse_iterator():
        # weight tree for command tree must be in dict already
        weight_tree = weight_tree_for_command_tree[command_tree]
        for child_command_tree in command_tree.child_trees_with_dataframe():
            child_weight_tree = WeightTree(
                child_command_tree, computing_node_type_priority_list, parent_weight_tree=weight_tree
            )
            weight_tree.child_weight_trees.append(
                child_weight_tree
            )
            weight_tree_for_command_tree[child_command_tree] = child_weight_tree

        # separate calculation for awaited trees
        for awaited_command_tree in command_tree.awaited_command_trees:
            define_computing_node_type_for_command_tree(
                awaited_command_tree, computing_node_type_priority_list, command_name_set
            )

    # calculate weights
    for weight_tree in root_weight_tree.children_first_order_traverse_iterator():
        _derive_weight_from_child_weights(weight_tree, command_name_set)

    # define computing node type for each command tree with best weight
    for weight_tree in root_weight_tree.parent_first_order_traverse_iterator():
        _define_computing_node_type(weight_tree, computing_node_type_priority_list)



