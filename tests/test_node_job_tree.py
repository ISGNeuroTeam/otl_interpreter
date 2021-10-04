from functools import partial
from py_otl_parser import Parser


from rest.test import TestCase

from otl_interpreter.interpreter_db import node_commands_manager
from otl_interpreter.job_planner.node_job_tree import make_node_job_tree
from otl_interpreter.job_planner.command_tree_constructor import make_command_tree

from register_test_commands import register_test_commands


class TestNodeJobTree(TestCase):
    def setUp(self):
        register_test_commands()
        self.command_syntax = node_commands_manager.get_commands_syntax()
        self.parser = Parser()
        self.parse = partial(self.parser.parse, syntax=self.command_syntax)

    def get_command_tree_from_otl(self, otl):
        translated_otl_commands = self.parse(otl)
        return make_command_tree(translated_otl_commands)

    def test_node_job_tree_creation_for_several_otl_queries(self):
        test_otl = "| otstats index='test_index' \
        | join [\
                | readfile 23,3,4 | sum 4,3,4,3,3,3\
                | merge_dataframes [ | readfile 1,2,3]  \
                | async name=test_async, [readfile 23,5,4 | collect index='test'] \
               ] \
        | table asdf,34,34,key=34 | await name=test_async |  merge_dataframes [ | readfile 1,2,3]"

        top_command_tree = self.get_command_tree_from_otl(test_otl)

        top_node_job_tree = make_node_job_tree(top_command_tree)

        # check that command tree in one node_job_tree
        command_tree_set = set()

        for node_job_tree in top_node_job_tree.parent_first_order_traverse_iterator():
            for command_tree in node_job_tree.command_tree.parent_first_order_traverse_iterator():
                self.assertNotIn(command_tree, command_tree_set)
                command_tree_set.add(command_tree)

        # check that every command tree has node job tree

        for command_tree in top_command_tree.parent_first_order_traverse_iterator('all_child_trees'):
            self.assertIn(command_tree, command_tree_set)
