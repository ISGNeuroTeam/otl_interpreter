class CommandTree(object):

    def __init__(
            self, command,
            pipeline_command_tree=None,
            subsearch_command_trees=None, 
            awaited_command_trees=None,
            next_command_tree=None
    ):
        """
        :param command: Command object
        :param pipeline_command_tree: Command Tree from  pipeline
        :param subsearch_command_trees: list of commands awaited for dataframe 
        :param awaited_command_trees: awaited list of command trees
        :param next_command_tree: next Command object for execution
        """
        self.command = command
        self.pipeline_command_tree = None
        self.subsearch_command_trees = []
        self.awaited_command_trees = []
        self.next_command_tree = None

        if pipeline_command_tree is not None:
            self.set_pipeline_command_tree(pipeline_command_tree)

        if subsearch_command_trees is not None:
            for subsearch_command in subsearch_command_trees:
                self.add_subsearch_command_tree(subsearch_command)

        if awaited_command_trees is not None:
            for awaited_command_tree in awaited_command_trees:
                self.add_awaited_command_tree(awaited_command_tree)

        if next_command_tree is not None:
            self.set_next_command_tree(next_command_tree)

    def __repr__(self):
        return self.command

    def set_pipeline_command_tree(self, pipeline_command_tree):
        self.pipeline_command_tree = pipeline_command_tree

    def add_subsearch_command_tree(self, subsearch_command_trees):
        self.subsearch_command_trees.append(subsearch_command_trees)

    def add_awaited_command_tree(self, awaited_command_tree):
        self.awaited_command_trees.append(awaited_command_tree)

    def set_next_command_tree(self, next_command_tree):
        self.next_command_tree = next_command_tree

