from otl_interpreter.interpreter_db.enums import ResultStorage


class ResultAddress:
    def __init__(self, node_job_tree, storage_type=None):
        """
        :param node_job_tree: node_job_tree which creates result dataframe
        :param storage_type: interproc, post processing storage, indexes
        """
        self.node_job_tree = node_job_tree
        self._path = None

        if storage_type is None:
            self._storage_type = ResultStorage.INTERPROCESSING.value

        else:
            self._storage_type = storage_type

        # read or write commands that will be use the address
        self._commands = set()

    def add_command(self, command):
        """
        add command for uding the address
        :param command:
        :return:
        """

        self._commands.add(command)

    def set_path(self, path, storage_type=None):
        """
        for all commands set path
        must be invoked once
        :return:
        """
        assert self._path is None

        self._path = path
        if storage_type is not None:
            self._storage_type = storage_type

        # set path for every command in
        for command in self._commands:
            command.set_path(self._path, self._storage_type)
