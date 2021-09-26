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
        self.command_pipeline_state.set_awaited_command_to_command_tree(command3)
        self.assertEqual(len(self.command_pipeline_state.awaited_command_trees), 0)
        self.assertEqual(len(command3.awaited_command_trees), 2)

    def test_add_command_tree_in_pipeline(self):
        command1 = CommandTree(Command('command1'))
        command2 = CommandTree(Command('command2'))
        self.command_pipeline_state.add_command_tree_in_pipeline(command1)
        self.assertIs(self.command_pipeline_state.previous_command_tree_in_pipeline, command1)
        self.command_pipeline_state.add_command_tree_in_pipeline(command2)
        self.assertIs(self.command_pipeline_state.previous_command_tree_in_pipeline, command2)
        self.assertIs(command1.next_command_tree, command2)


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








