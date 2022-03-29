from otl_interpreter.interpreter_db.enums import ResultStorage


class ResultAddress:
    def __init__(self, node_job_tree, storage_type=None):
        """
        :param node_job_tree: node_job_tree which creates result dataframe
        :param storage_type: interproc, post processing locale storage or postprocessing shared storage
        """
        self.node_job_tree = node_job_tree
        self._path = None

        if storage_type is None:
            self.storage_type = ResultStorage.INTERPROCESSING.value

        else:
            self.storage_type = storage_type

        # read or write commands that will be use the address
        self._commands = list()

        # add top command of node job tree to _commands
        top_node_job_command = node_job_tree.get_top_command()
        self.add_command(top_node_job_command)
        node_job_tree.set_result_address(self)

    @property
    def path(self):
        return self._path

    def add_command(self, command):
        """
        add command for using the address
        :param command:
        :return:
        """
        assert command not in self._commands
        self._commands.append(command)

    def set_path(self, path, storage_type=None):
        """
        for all commands set path
        must be invoked once
        :return:
        """
        assert self._path is None

        self._path = path
        if storage_type is not None:
            self.storage_type = storage_type

        # set path for every command in
        for command in self._commands:
            command.set_path(self._path, self.storage_type)
