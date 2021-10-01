from .command import Command
from .command_tree import CommandTree
from .exceptions import JobPlanException


class CommandPipelineState:
    def __init__(self):
        # dataframe from previous_command_tree_in_pipeline goes to the next command tree in the command pipeline
        # await command can override it
        self.previous_command_tree_in_pipeline = None

        # awaited commands trees from async subsearches.
        self.awaited_command_trees = []

    def override_previous_command_tree_in_pipeline(self, command_tree):
        """
        Overrides previous_command_tree_in_pipeline
        put previous_command_tree_in_pipeline  in awaited_coommand_trees
        """
        if self.previous_command_tree_in_pipeline is not None:
            self.awaited_command_trees.append(self.previous_command_tree_in_pipeline)
        self.previous_command_tree_in_pipeline = command_tree

    def add_awaited_command_tree(self, command_tree):
        """
        Save command_tree that should be awaited
        """
        self.awaited_command_trees.append(command_tree)

    def add_awaited_command_tree_list(self, command_tree_list):
        """
        Save several command_tree that should be awaited
        """
        self.awaited_command_trees.extend(command_tree_list)

    def add_command_tree_in_pipeline(self, command_tree):
        """
        Make link between self.previous_command_tree_in_pipeline and command_tree
        Set previous_command_tree_in_pipeline and next_command_tree_in_pipeline
        """
        # all current awaited command link to command tree
        self._set_awaited_command_to_command_tree(command_tree)

        if self.previous_command_tree_in_pipeline is not None:
            self.previous_command_tree_in_pipeline.set_next_command_tree_in_pipeline(
                command_tree
            )
            command_tree.set_previous_command_tree_in_pipeline(
                self.previous_command_tree_in_pipeline
            )

        self.previous_command_tree_in_pipeline = command_tree

    def _set_awaited_command_to_command_tree(self, command_tree):
        """
        All self.awaited_command_trees  append to command tree
        :param command_tree:
        :return:
        """
        for awaited_command_tree in self.awaited_command_trees:
            awaited_command_tree.set_next_command_tree_outside_pipeline(command_tree)
            command_tree.add_awaited_command_tree(awaited_command_tree)
        self.awaited_command_trees = []


class CommandTreeConstructor:
    def __init__(self):
        self.async_subsearches = {}
        self.processed_async_subsearches = set()

    def create_command_tree(self, translated_otl_commands):
        """
        :param translated_otl_commands: translated otl, list of dictionaries
        :return:
        tuple: CommandTree object, awaited_command_tree_list
        """
        command_pipeline_state = CommandPipelineState()

        for translated_otl_command in translated_otl_commands:
            if self._is_async(translated_otl_command):
                self._process_async_command(translated_otl_command)

            elif self._is_await(translated_otl_command):
                self._process_await_command(translated_otl_command, command_pipeline_state)

            else:
                self._process_ordinary_command(translated_otl_command, command_pipeline_state)

        return command_pipeline_state.previous_command_tree_in_pipeline,\
            command_pipeline_state.awaited_command_trees

    def _process_await_command(self, translated_otl_command, command_pipeline_state):
        await_name = self._get_await_name(translated_otl_command)

        if await_name in self.async_subsearches:

            async_subsearch_tree, subsearch_awaited_command_trees = \
                self.create_command_tree(self.async_subsearches[await_name])

            self.processed_async_subsearches.add(await_name)
            del self.async_subsearches[await_name]

        else:
            if await_name in self.processed_async_subsearches:
                raise JobPlanException(f'Second await with name <{await_name}> is not allowed')
            else:
                raise JobPlanException(f'Not found async command with name <{await_name}> for await command')

        # await override=True means that dataframe goes to next command
        # and previous command just awaited
        if self._is_await_with_override(translated_otl_command):
            command_pipeline_state.override_previous_command_tree_in_pipeline(async_subsearch_tree)
        else:
            command_pipeline_state.add_awaited_command_tree(async_subsearch_tree)

        # all not linked awaited commands from subsearches goes further in pipeline
        if subsearch_awaited_command_trees:
            command_pipeline_state.add_awaited_command_tree_list(subsearch_awaited_command_trees)

    def _process_async_command(self, translated_otl_command):
        """
        put async subsearch in set for later processing with await
        """
        async_name = self._get_async_name(translated_otl_command)
        async_subsearch = self._get_async_subsearch(translated_otl_command)
        if async_name in self.async_subsearches:
            raise JobPlanException(f'Two async job with name <{async_name}>')
        self.async_subsearches[async_name] = async_subsearch

    def _process_ordinary_command(
            self, translated_otl_command, command_pipeline_state

    ):
        subseartches = self._get_subsearches(translated_otl_command)
        command = self._make_command(translated_otl_command)
        command_tree = CommandTree(command, command_pipeline_state.previous_command_tree_in_pipeline)

        # collect all awaited command trees from subsearches
        awaited_command_trees_from_subsearches = []

        for subsearch in subseartches:
            subsearch_tree, subsearch_awaited_command_trees = self.create_command_tree(subsearch)
            command_tree.add_subsearch_command_tree(subsearch_tree)
            awaited_command_trees_from_subsearches.extend(subsearch_awaited_command_trees)

        command_pipeline_state.add_command_tree_in_pipeline(command_tree)

        # all not linked awaited commands from subsearches goes further in pipeline
        command_pipeline_state.add_awaited_command_tree_list(
            awaited_command_trees_from_subsearches
        )

    @staticmethod
    def _is_async(translated_otl_command):
        return translated_otl_command['command']['value'] == 'async'

    def _get_async_name(self, translated_otl_command):
        return self._get_kwarg_by_name(translated_otl_command, 'name')

    def _get_async_subsearch(self, translated_otl_command):
        return self._get_subsearches(translated_otl_command)[0]

    @staticmethod
    def _is_await(translated_otl_command):
        return translated_otl_command['command']['value'] == 'await'

    def _is_await_with_override(self, translated_otl_command):
        override = self._get_kwarg_by_name(translated_otl_command, 'override')
        return override is not None and override.lower() == 'true'

    def _get_await_name(self, translated_otl_command):
        return self._get_kwarg_by_name(translated_otl_command, 'name')

    @staticmethod
    def _get_kwarg_by_name(translated_otl_command, kwarg_name):
        for arg in translated_otl_command['commandargs']:
            if arg['type'] == 'kwarg' and arg['key']['value'] == kwarg_name:
                return arg['value']['value']['value']
        return None

    @staticmethod
    def _get_subsearches(translated_otl_command):
        subsearches = []
        for arg in translated_otl_command['commandargs']:
            if arg['type'] == 'subsearch':
                subsearches.append(arg['value'])
        return subsearches

    @staticmethod
    def _make_command(translated_otl_command):
        # remove subsearches
        translated_otl_command['commandargs'] = list(filter(
            lambda arg: arg['type'] != 'subsearch',
            translated_otl_command['commandargs']
        ))
        return Command(translated_otl_command)


def construct_command_tree_from_translated_otl_commands(translated_otl_commands):
    """
    :param translated_otl_commands: list of translated otl commands
    :return:
    tuple: command_tree, list of awaited command trees
    """
    constructor = CommandTreeConstructor()
    command_tree, awaited_command_trees = constructor.create_command_tree(translated_otl_commands)

    if constructor.async_subsearches:
        raise JobPlanException(f"Async subsearches with names {' '.join(constructor.async_subsearches.keys())}")

    return command_tree, awaited_command_trees
