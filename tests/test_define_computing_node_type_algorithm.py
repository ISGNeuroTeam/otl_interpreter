from rest.test import TestCase
from otl_interpreter.job_planner.define_computing_node_type_algorithm import (
    _find_next_min_weight_for_node_type,
    _find_computing_node_type_for_parent_node_type,
    define_computing_node_type_for_command_tree,
    WeightTree
)
from otl_interpreter.settings import ini_config

from otl_interpreter.interpreter_db import node_commands_manager
from otl_interpreter.job_planner.command_tree_constructor import CommandTreeConstructor, make_command_tree
from otl_interpreter.job_planner import JobPlanner

from register_test_commands import register_test_commands


class TestWeightTree(TestCase):
    def setUp(self):

        # database created after import
        # so we need import translate_otl after test database creations
        from otl_interpreter.translator import translate_otl
        self.translate_otl = translate_otl
        register_test_commands()

        self.computing_node_type_priority_list = ini_config['job_planer']['computing_node_type_priority'].split()
        self.command_name_sets = {
            computing_node_type:
                node_commands_manager.get_command_name_set_for_node_type(computing_node_type)
            for computing_node_type in self.computing_node_type_priority_list
        }

    def get_command_tree_from_otl(self, otl):
        translated_otl_commands = self.translate_otl(otl)
        command_tree_constructor = CommandTreeConstructor()
        command_tree, awaited_command_trees_list = command_tree_constructor.create_command_tree(translated_otl_commands)
        return command_tree, awaited_command_trees_list

    def test_find_min_weight_for_node_type(self):
        weights = {
            'SPARK': 4,
            'EEP': 3,
            'POST_PROCESSING': 1
        }

        self.assertEqual(_find_next_min_weight_for_node_type('SPARK', weights), 2)
        self.assertEqual(_find_next_min_weight_for_node_type('EEP', weights), 2)
        self.assertEqual(_find_next_min_weight_for_node_type('POST_PROCESSING', weights), 1)

        weights = {
            'SPARK': 4,
            'EEP': 3,
            'POST_PROCESSING': 4
        }

        self.assertEqual(_find_next_min_weight_for_node_type('SPARK', weights), 4)
        self.assertEqual(_find_next_min_weight_for_node_type('EEP', weights), 3)
        self.assertEqual(_find_next_min_weight_for_node_type('POST_PROCESSING', weights), 4)

    def test_find_computing_node_type_for_parent_node_type(self):
        priority_list = self.computing_node_type_priority_list
        weights = {
            'SPARK': 4,
            'EEP': 3,
            'POST_PROCESSING': 1
        }
        parent_computing_node_type = 'SPARK'
        self.assertEqual(
            _find_computing_node_type_for_parent_node_type(
                parent_computing_node_type, weights, priority_list
            ),
            'POST_PROCESSING'
        )

        weights = {
            'SPARK': 4,
            'EEP': 3,
            'POST_PROCESSING': 3
        }
        parent_computing_node_type = 'SPARK'
        self.assertEqual(
            _find_computing_node_type_for_parent_node_type(
                parent_computing_node_type, weights, priority_list
            ),
            'SPARK'
        )

        weights = {
            'SPARK': 5,
            'EEP': 3,
            'POST_PROCESSING': 3
        }
        parent_computing_node_type = 'SPARK'
        self.assertEqual(
            _find_computing_node_type_for_parent_node_type(
                parent_computing_node_type, weights, priority_list
            ),
            'EEP'
        )

        weights = {
            'SPARK': 4,
            'EEP': 3,
            'POST_PROCESSING': 3
        }
        parent_computing_node_type = 'SPARK'
        self.assertEqual(
            _find_computing_node_type_for_parent_node_type(
                parent_computing_node_type, weights, ['EEP', 'SPARK', 'POST_PROCESSING']
            ),
            'EEP'
        )

        weights = {
            'SPARK': 3,
            'EEP': 3,
            'POST_PROCESSING': 2
        }
        parent_computing_node_type = 'EEP'
        self.assertEqual(
            _find_computing_node_type_for_parent_node_type(
                parent_computing_node_type, weights, priority_list
            ),
            'EEP'
        )

    def test_parent_first_order_traverse_iterator(self):
        test_otl = "| otstats index='test_index' \
        | join [ | readfile 23,3,4 | sum 4,3,4,3,3,3  | merge_dataframes [| readfile 1,2,3] ] \
        | table asdf,34,34,key=34"

        command_tree, awaited_command_trees_list = self.get_command_tree_from_otl(test_otl)
        root_weight_tree = WeightTree(
            command_tree, self.computing_node_type_priority_list
        )

        visited = set()

        def visit(weight_tree):
            visited.add(weight_tree)
            if weight_tree.parent_weight_tree:
                self.assertIn(weight_tree.parent_weight_tree, visited)

            # print(id(weight_tree))
            # print('\t\t' + ' '.join([str(id(child)) for child in weight_tree.child_weight_trees]))

        for weight_tree in root_weight_tree.parent_first_order_traverse_iterator():
            visit(weight_tree)

    def test_children_first_order_traverse_iterator(self):
        test_otl = "| otstats index='test_index' \
        | join [ | readfile 23,3,4 | sum 4,3,4,3,3,3  | merge_dataframes [| readfile 1,2,3] ] \
        | table asdf,34,34,key=34 | otstats index='test_index' \
        | join [ | readfile 23,3,4 | sum 4,3,4,3,3,3  | merge_dataframes [| readfile 1,2,3] ] \
        | table asdf,34,34,key=34"

        command_tree, awaited_command_trees_list = self.get_command_tree_from_otl(test_otl)
        root_weight_tree = WeightTree(
            command_tree, self.computing_node_type_priority_list
        )

        visited = set()

        def visit(weight_tree):
            visited.add(weight_tree)
            for child_weight_tree in  weight_tree.child_weight_trees:
                self.assertIn(child_weight_tree, visited)

            # print(id(weight_tree))
            # print('\t\t' + ' '.join([str(id(child)) for child in weight_tree.child_weight_trees]))

        for weight_tree in root_weight_tree.children_first_order_traverse_iterator():
            visit(weight_tree)

    def test_define_computing_node_type_for_command_tree(self):
        test_otl = "| otstats index='test_index' | join [ readfile 1,2,3 ] | merge_dataframes [| readfile 2,3,4]"

        command_tree, awaited_command_trees_list = self.get_command_tree_from_otl(test_otl)

        define_computing_node_type_for_command_tree(
            command_tree, self.computing_node_type_priority_list, self.command_name_sets
        )

        self.assertEqual(command_tree.computing_node_type, 'EEP')
        self.assertEqual(command_tree.subsearch_command_trees[0].computing_node_type, 'SPARK')
        self.assertEqual(command_tree.previous_command_tree_in_pipeline.computing_node_type, 'SPARK')

    def test_define_computing_node_async_await(self):
        test_otl = "| otstats index='test_index' \
               | join [\
                       | readfile 23,3,4 | sum 4,3,4,3,3,3\
                       | merge_dataframes [ | readfile 1,2,3]  \
                       | async name=test_async, [readfile 23,5,4 | collect index='test'] \
                      ] \
               | table asdf,34,34,key=34 | await name=test_async, override=True |  merge_dataframes [ | readfile 1,2,3]"

        root_command_tree = make_command_tree(self.translate_otl(test_otl))

        define_computing_node_type_for_command_tree(
            root_command_tree, self.computing_node_type_priority_list, self.command_name_sets
        )

        # make sure that all nodes in command tree have defined computing node type
        for command_tree in root_command_tree.parent_first_order_traverse_iterator('all_child_trees'):
            self.assertIsNotNone(command_tree.computing_node_type)

        self.assertEqual(
            root_command_tree.first_command_tree_in_pipeline.computing_node_type,
            'SPARK'
        )