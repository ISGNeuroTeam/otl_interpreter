from .abstract_tree import AbstractTree


class CommandTree(AbstractTree):

    def __init__(
            self, command,
            previous_command_tree_in_pipeline=None,
            subsearch_command_trees=None,
            awaited_command_trees=None,
            next_command_tree_in_pipeline=None,
            next_command_tree_outside_pipeline=None
    ):
        """
        :param command: Command object
        :param previous_command_tree_in_pipeline: Command Tree from  pipeline
        :param subsearch_command_trees: list of commands awaited for dataframe 
        :param awaited_command_trees: awaited list of command trees
        :param next_command_tree_in_pipeline: next Command object for execution
        """
        self.command = command
        self.previous_command_tree_in_pipeline = None
        self.subsearch_command_trees = []
        self.awaited_command_trees = []

        self.next_command_tree_in_pipeline = None
        # next command tree that awaiting current command tree
        self.next_command_tree_outside_pipeline = None

        # if previous command tree in pipeline is None then current command tree is first command tree in pipeline
        self.first_command_tree_in_pipeline = self

        self.computing_node_type = None

        if previous_command_tree_in_pipeline is not None:
            self.set_previous_command_tree_in_pipeline(previous_command_tree_in_pipeline)

        if subsearch_command_trees is not None:
            for subsearch_command in subsearch_command_trees:
                self.add_subsearch_command_tree(subsearch_command)

        if awaited_command_trees is not None:
            for awaited_command_tree in awaited_command_trees:
                self.add_awaited_command_tree(awaited_command_tree)

        if next_command_tree_in_pipeline is not None:
            self.set_next_command_tree_in_pipeline(next_command_tree_in_pipeline)

        if next_command_tree_outside_pipeline is not None:
            self.set_next_command_tree_outside_pipeline(next_command_tree_outside_pipeline)

    @property
    def children(self):
        return self.child_trees_with_dataframe()

    @property
    def parent(self):
        return self.next_command_tree_in_pipeline or self.next_command_tree_outside_pipeline

    @property
    def next_command_tree(self):
        if self.next_command_tree_in_pipeline is not None:
            return self.next_command_tree_in_pipeline
        elif self.next_command_tree_outside_pipeline is not None:
            return self.next_command_tree_outside_pipeline

        else:
            return None

    def __repr__(self):
        result = f'Command: {self.command.name}'
        if self.previous_command_tree_in_pipeline:
            result  = result + f',prev: {self.previous_command_tree_in_pipeline.command.name} '
        return result

    def set_previous_command_tree_in_pipeline(self, previous_command_tree_in_pipeline):
        # remove next command tree property from previous command if it refers to current command
        if self.previous_command_tree_in_pipeline is not None and \
                self.previous_command_tree_in_pipeline.next_command_tree_in_pipeline is self:
            self.previous_command_tree_in_pipeline.next_command_tree_in_pipeline = None

        self.previous_command_tree_in_pipeline = previous_command_tree_in_pipeline
        self.previous_command_tree_in_pipeline.next_command_tree_in_pipeline = self

        self.set_first_command_tree_in_pipeline(
            previous_command_tree_in_pipeline.first_command_tree_in_pipeline
        )

    def set_first_command_tree_in_pipeline(self, command_tree):
        self.first_command_tree_in_pipeline = command_tree

        # change all first_command_tree_in_pipeline  attributes in pipeline
        current_command_tree = self.next_command_tree_in_pipeline
        while current_command_tree is not None:
            current_command_tree.first_command_tree_in_pipeline = command_tree
            current_command_tree = current_command_tree.next_command_tree_in_pipeline

    def add_subsearch_command_tree(self, subsearch_command_trees):
        self.subsearch_command_trees.append(subsearch_command_trees)

    def replace_subsearch_command_tree(self, subsearch_command_tree, index):
        self.subsearch_command_trees[index] = subsearch_command_tree

    def add_awaited_command_tree(self, awaited_command_tree):
        self.awaited_command_trees.append(awaited_command_tree)
        awaited_command_tree.next_command_tree_outside_pipeline = self

    def set_next_command_tree_in_pipeline(self, next_command_tree_in_pipeline):
        assert self.next_command_tree_outside_pipeline is None
        next_command_tree_in_pipeline.set_previous_command_tree_in_pipeline(self)

    def set_next_command_tree_outside_pipeline(self, next_command_tree_outside_pipeline):
        assert self.next_command_tree_in_pipeline is None
        if self not in next_command_tree_outside_pipeline.awaited_command_trees:
            next_command_tree_outside_pipeline.add_awaited_command_tree(self)

    def set_computing_node_type(self, computing_node_type):
        self.computing_node_type = computing_node_type

    def child_trees_with_dataframe(self):
        """
        Generator. Traverse through previsous_command_tree in pipeline and all subsearch trees
        """
        if self.previous_command_tree_in_pipeline:
            yield self.previous_command_tree_in_pipeline

        for command_tree in self.subsearch_command_trees:
            yield command_tree

    def all_child_trees(self):
        """
        Generator. Traverse through all child trees
        """
        if self.previous_command_tree_in_pipeline:
            yield self.previous_command_tree_in_pipeline

        for command_tree in self.subsearch_command_trees:
            yield command_tree

        for command_tree in self.awaited_command_trees:
            yield command_tree

    def through_pipeline_iterator(self):
        """
        Generator. Traverse from first command tree  in pipeline tree to current command tree
        """
        yield self.first_command_tree_in_pipeline
        current_command_tree = self.first_command_tree_in_pipeline.next_command_tree_in_pipeline

        while current_command_tree is not None:
            yield current_command_tree
            current_command_tree = current_command_tree.next_command_tree_in_pipeline

    def as_command_dict(self):
        return {
            'name': self.command.name,
            'subsearches': [subsearch.as_command_dict() for subsearch in self.subsearch_command_trees],
            'arguments': self.command.args,
        }

