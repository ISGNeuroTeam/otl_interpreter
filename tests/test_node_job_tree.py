from pprint import pp

from translate_otl import translate_otl


from rest.test import TestCase

from otl_interpreter.interpreter_db import node_commands_manager
from otl_interpreter.job_planner.node_job_tree import make_node_job_tree
from otl_interpreter.job_planner.command_tree_constructor import make_command_tree
from otl_interpreter.job_planner.define_computing_node_type_algorithm import define_computing_node_type_for_command_tree

from register_test_commands import register_test_commands


class TestNodeJobTree(TestCase):
    def setUp(self):
        register_test_commands()

        self.computing_node_type_priority_list = ['SPARK', 'EEP', 'POST_PROCESSING']
        self.command_name_sets = {
            computing_node_type:
                node_commands_manager.get_command_name_set_for_node_type(computing_node_type)
            for computing_node_type in self.computing_node_type_priority_list
        }

    def get_command_tree_from_otl(self, otl):
        translated_otl_commands = translate_otl(otl)
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

        # for command_tree in top_command_tree.through_pipeline_iterator():
        #     print(command_tree.command.name)

        all_command_tree = [command_tree for command_tree in top_command_tree.parent_first_order_traverse_iterator('all_child_trees')]

        # print('--all--command--trees')
        # for command_tree in all_command_tree:
        #     print(command_tree.command.name)

        define_computing_node_type_for_command_tree(
            top_command_tree, self.computing_node_type_priority_list, self.command_name_sets
        )

        # print('---defines--comp--nodes')
        # for command_tree in top_command_tree.through_pipeline_iterator():
        #     print(command_tree.command.name)
        #     print(command_tree.computing_node_type)
        #
        # print('------------------------')
        self.assertEqual(
            top_command_tree.first_command_tree_in_pipeline.computing_node_type,
            'SPARK'
        )

        self.assertEqual(
            top_command_tree.first_command_tree_in_pipeline.command.name,
            'otstats'
        )

        self.assertEqual(
            top_command_tree.first_command_tree_in_pipeline.next_command_tree_in_pipeline.command.name,
            'join'
        )

        top_node_job_tree = make_node_job_tree(top_command_tree)

        # check that command tree in one node_job_tree
        node_job_command_tree_set = set()

        for node_job_tree in top_node_job_tree.parent_first_order_traverse_iterator():
            for command_tree in node_job_tree.command_tree.parent_first_order_traverse_iterator():
                if command_tree in node_job_command_tree_set:
                    self.assertNotIn(command_tree, node_job_command_tree_set)
                node_job_command_tree_set.add(command_tree)

        # check that every command tree has node job tree
        for command_tree in all_command_tree:
            self.assertIn(command_tree, node_job_command_tree_set)

        # print('--all--command--trees--in node jobs')
        # for command_tree in node_job_command_tree_set:
        #     print(command_tree.command.name)
        # print('------------------------------------')

        # for node_job_tree in top_node_job_tree.parent_first_order_traverse_iterator():
        #     print('-----NodeJob')
        #     print(node_job_tree.computing_node_type)
        #     print('top command: ' + node_job_tree.command_tree.command.name)
        #     print(
        #         'first_command_in_pipeline: ' + node_job_tree.command_tree.first_command_tree_in_pipeline.command.name)
        #     pp(node_job_tree.as_command_dict_list())
        #     print('-----------------')

