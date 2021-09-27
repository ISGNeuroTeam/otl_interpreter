from functools import partial
from py_otl_parser import Parser
from rest.test import TestCase

from otl_interpreter.job_planner.command_tree_constructor import CommandPipelineState, CommandTreeConstructor
from otl_interpreter.job_planner.command import Command
from otl_interpreter.job_planner.command_tree import CommandTree

from register_test_commands import register_test_commands
from otl_interpreter.interpreter_db import node_commands_manager


class TestCommandPipelineState(TestCase):
    def setUp(self):
        self.command_pipeline_state = CommandPipelineState()

    def test_add_awaited_command_tree(self):
        command1 = CommandTree(Command('command1'))
        command2 = CommandTree(Command('command2'))
        command3 = CommandTree(Command('command3'))

        self.assertEqual(len(self.command_pipeline_state.awaited_command_trees), 0)
        self.command_pipeline_state.add_awaited_command_tree(command1)
        self.assertEqual(len(self.command_pipeline_state.awaited_command_trees), 1)
        self.command_pipeline_state.add_awaited_command_tree(command2)
        self.assertEqual(len(self.command_pipeline_state.awaited_command_trees), 2)
        self.command_pipeline_state.add_awaited_command_tree(command3)
        self.assertEqual(len(self.command_pipeline_state.awaited_command_trees), 3)

    def test_override_previous_command_tree_in_pipeline(self):
        command1 = CommandTree(Command('command1'))
        command2 = CommandTree(Command('command2'))
        command3 = CommandTree(Command('command3'))
        command4 = CommandTree(Command('command4'))

        self.command_pipeline_state.add_command_tree_in_pipeline(command1)

        self.command_pipeline_state.add_awaited_command_tree(command2)
        self.command_pipeline_state.add_awaited_command_tree(command3)

        self.command_pipeline_state.override_previous_command_tree_in_pipeline(command4)

        self.assertEqual(len(self.command_pipeline_state.awaited_command_trees), 3)
        self.assertIs(self.command_pipeline_state.previous_command_tree_in_pipeline, command4)
        self.assertIn(command1, self.command_pipeline_state.awaited_command_trees)

    def test_set_awaited_command_to_command_tree(self):
        command1 = CommandTree(Command('command1'))
        command2 = CommandTree(Command('command2'))
        command3 = CommandTree(Command('command3'))

        self.command_pipeline_state.add_awaited_command_tree(command1)
        self.command_pipeline_state.add_awaited_command_tree(command2)
        self.assertEqual(len(self.command_pipeline_state.awaited_command_trees), 2)
        self.command_pipeline_state._set_awaited_command_to_command_tree(command3)
        self.assertEqual(len(self.command_pipeline_state.awaited_command_trees), 0)
        self.assertEqual(len(command3.awaited_command_trees), 2)

    def test_add_command_tree_in_pipeline(self):
        command1 = CommandTree(Command('command1'))
        command2 = CommandTree(Command('command2'))
        self.command_pipeline_state.add_command_tree_in_pipeline(command1)
        self.assertIs(self.command_pipeline_state.previous_command_tree_in_pipeline, command1)
        self.command_pipeline_state.add_command_tree_in_pipeline(command2)
        self.assertIs(self.command_pipeline_state.previous_command_tree_in_pipeline, command2)
        self.assertIs(command1.next_command_tree_in_pipeline, command2)


class TestCommandTreeConstructorInnerMethods(TestCase):
    def setUp(self):
        register_test_commands()
        self.command_syntax = node_commands_manager.get_commands_syntax()
        self.parser = Parser()
        self.parse = partial(self.parser.parse, syntax=self.command_syntax)

    def test_get_kwarg_by_name(self):
        test_otl = "| async name=s3, [|readfile 1, 2, 3 ] | table a, b, c, key=val"
        translated_otl_commands = self.parse(test_otl)
        command_tree_constructor = CommandTreeConstructor()
        self.assertEqual(
            command_tree_constructor._get_kwarg_by_name(translated_otl_commands[0], 'name'),
            's3'
        )
        self.assertEqual(
            command_tree_constructor._get_kwarg_by_name(translated_otl_commands[1], 'key'),
            'val'
        )
        self.assertIsNone(
            command_tree_constructor._get_kwarg_by_name(translated_otl_commands[1], 'key2'),
        )

    def test_get_subsearches(self):
        test_otl = "| readfile 1, 2, 3 | \
            merge_dataframes [| otstats index='test1' | sum 2,3,4,5], [| readfile 3,4,5], [| otstats index='test2']"
        translated_otl_commands = self.parse(test_otl)
        command_tree_constructor = CommandTreeConstructor()
        subsearches = command_tree_constructor._get_subsearches(translated_otl_commands[1])
        self.assertEqual(len(subsearches), 3)
        self.assertEqual(subsearches[0][0]['command']['value'], 'otstats')
        self.assertEqual(subsearches[1][0]['command']['value'], 'readfile')
        self.assertEqual(subsearches[2][0]['command']['value'], 'otstats')

    def test_get_await_name(self):
        test_otl = "| await name=\"test_name\" | readfile 4,5,3"
        translated_otl_commands = self.parse(test_otl)
        command_tree_constructor = CommandTreeConstructor()

        self.assertEqual(command_tree_constructor._get_await_name(translated_otl_commands[0]), "\"test_name\"")

    def test_get_async_name(self):
        test_otl = "| readfile 4,5,3 | async name=\"test_name\", [| otstats index='test']"
        translated_otl_commands = self.parse(test_otl)
        command_tree_constructor = CommandTreeConstructor()

        self.assertEqual(command_tree_constructor._get_await_name(translated_otl_commands[1]), "\"test_name\"")

    def test_is_async(self):
        test_otl = "| readfile 4,5,3 | async name=\"test_name\", [| otstats index='test']"
        translated_otl_commands = self.parse(test_otl)
        command_tree_constructor = CommandTreeConstructor()
        self.assertEqual(
            command_tree_constructor._is_async(translated_otl_commands[1]),
            True
        )

    def test_is_await(self):
        test_otl = "| await name=\"test_name\" | readfile 4,5,3"
        translated_otl_commands = self.parse(test_otl)
        command_tree_constructor = CommandTreeConstructor()

        self.assertEqual(
            command_tree_constructor._is_await(translated_otl_commands[0]),
            True
        )


class TestCommandTree(TestCase):
    def setUp(self):
        register_test_commands()
        self.command_syntax = node_commands_manager.get_commands_syntax()
        self.parser = Parser()
        self.parse = partial(self.parser.parse, syntax=self.command_syntax)

    def get_command_tree_from_otl(self, otl):
        translated_otl_commands = self.parse(otl)
        command_tree_constructor = CommandTreeConstructor()
        command_tree, awaited_command_trees_list = command_tree_constructor.create_command_tree(translated_otl_commands)
        return command_tree, awaited_command_trees_list

    def test_simple_tree(self):
        test_otl = "| otstats index='test_index' \
        | join [ | readfile 23,3,4 | sum 4,3,4,3,3,3  | merge_dataframes [| readfile 1,2,3] ] \
        | table asdf,34,34,key=34"

        command_tree, awaited_command_trees_list = self.get_command_tree_from_otl(test_otl)

        # there is no async commands
        self.assertEqual(len(awaited_command_trees_list), 0)

        # top is table command
        self.assertEqual(command_tree.command.name, 'table')

        join_command_tree = command_tree.previous_command_tree_in_pipeline

        self.assertEqual(join_command_tree.command.name, 'join')
        self.assertEqual(len(join_command_tree.subsearch_command_trees), 1)
        self.assertEqual(join_command_tree.subsearch_command_trees[0].command.name, 'merge_dataframes')
        self.assertEqual(command_tree.first_command_tree_in_pipeline.command.name, 'otstats')

    def test_tree_with_async(self):
        test_otl = "| otstats index='test_index' \
        | join [\
                | readfile 23,3,4 | sum 4,3,4,3,3,3\
                | merge_dataframes [ | readfile 1,2,3]  \
                | async name=test_async, [readfile 23,5,4 | collect index='test'] \
               ] \
        | table asdf,34,34,key=34 | await name=test_async"

        command_tree, awaited_command_trees_list = self.get_command_tree_from_otl(test_otl)

        self.assertEqual(len(awaited_command_trees_list), 1)

        top_command_name_in_awaited_command_tree = awaited_command_trees_list[0].command.name
        self.assertEqual(top_command_name_in_awaited_command_tree, 'collect')

        first_command_name_in_awaited_pipeline = \
            awaited_command_trees_list[0].first_command_tree_in_pipeline.command.name

        self.assertEqual(first_command_name_in_awaited_pipeline, 'readfile')

        # check join subsearches
        self.assertEqual(
            command_tree.first_command_tree_in_pipeline.\
            next_command_tree_in_pipeline.subsearch_command_trees[0].command.name,
            'merge_dataframes'
        )

        self.assertEqual(
            command_tree.first_command_tree_in_pipeline.
            next_command_tree_in_pipeline.subsearch_command_trees[0].
            first_command_tree_in_pipeline.command.name,
            'readfile'
        )

    def test_tree_with_async_await(self):
        test_otl = "| otstats index='test_index' \
        | join [\
                | readfile 23,3,4 | sum 4,3,4,3,3,3\
                | merge_dataframes [ | readfile 1,2,3]  \
                | async name=test_async, [readfile 23,5,4 | collect index='test'] \
               ] \
        | table asdf,34,34,key=34 | await name=test_async |  merge_dataframes [ | readfile 1,2,3]"

        command_tree, awaited_command_trees_list = self.get_command_tree_from_otl(test_otl)

        self.assertEqual(len(awaited_command_trees_list), 0)
        self.assertEqual(len(command_tree.awaited_command_trees), 1)
        self.assertEqual(command_tree.awaited_command_trees[0].command.name, 'collect')

    def test_await_with_override(self):
        test_otl = "| otstats index='test_index' \
        | join [\
                | readfile 23,3,4 | sum 4,3,4,3,3,3\
                | merge_dataframes [ | readfile 1,2,3]  \
                | async name=test_async, [readfile 23,5,4 | collect index='test'] \
               ] \
        | table asdf,34,34,key=34 | await name=test_async, override=True |  merge_dataframes [ | readfile 1,2,3]"

        command_tree, awaited_command_trees_list = self.get_command_tree_from_otl(test_otl)

        self.assertEqual(command_tree.command.name, 'merge_dataframes')
        self.assertEqual(
            command_tree.previous_command_tree_in_pipeline.command.name,
            'collect'
        )
        self.assertEqual(
            command_tree.previous_command_tree_in_pipeline.first_command_tree_in_pipeline.command.name,
            'readfile'
        )
        self.assertEqual(
            len(awaited_command_trees_list),
            0
        )

        self.assertIs(
            command_tree.previous_command_tree_in_pipeline.next_command_tree_in_pipeline,
            command_tree
        )

        self.assertEqual(
            command_tree.awaited_command_trees[0].command.name,
            'table'
        )

        self.assertIs(
            command_tree.awaited_command_trees[0].next_command_tree_outside_pipeline,
            command_tree
        )

















