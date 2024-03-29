import datetime
from pprint import pp

from rest.test import TestCase

from otl_interpreter.interpreter_db import node_commands_manager
from otl_interpreter.interpreter_db.enums import ResultStorage
from otl_interpreter.job_planner.node_job_tree_constructor import _construct_node_job_tree, make_node_job_tree
from otl_interpreter.job_planner.command_tree_constructor import make_command_tree
from otl_interpreter.job_planner.define_computing_node_type_algorithm import define_computing_node_type_for_command_tree
from otl_interpreter.job_planner.sys_commands import SysReadWriteCommand
from otl_interpreter.translator import translate_otl

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
        self.default_tws = datetime.datetime.now() - datetime.timedelta(days=1)
        self.default_twf = datetime.datetime.now()

    def get_command_tree_from_otl(self, otl):
        translated_otl_commands = translate_otl(otl)
        return make_command_tree(translated_otl_commands)

    def get_node_job_tree_from_otl(self, otl, shared=True):
        top_command_tree = self.get_command_tree_from_otl(otl)
        define_computing_node_type_for_command_tree(
            top_command_tree, self.computing_node_type_priority_list, self.command_name_sets
        )
        return make_node_job_tree(top_command_tree, self.default_tws, self.default_twf, shared)

    def check_is_it_hash_string(self, s):
        char_set = set('0123456789abcdef')
        self.assertEqual(len(s), 128)
        self.assertSetEqual(set(s), char_set)

    def check_one_command_tree_in_one_node_job(self, all_command_trees, top_node_job_tree):

        # check that command tree in one node_job_tree
        node_job_command_tree_set = set()

        for node_job_tree in top_node_job_tree.parent_first_order_traverse_iterator():
            for command_tree in node_job_tree.command_tree.parent_first_order_traverse_iterator():
                if command_tree in node_job_command_tree_set:
                    self.assertNotIn(command_tree, node_job_command_tree_set)
                node_job_command_tree_set.add(command_tree)

        # check that every command tree has node job tree
        for command_tree in all_command_trees:
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

    def test_define_computing_node_type(self):
        test_otl = "| otstats index='test_index' \
                        | join [\
                                | readfile 23,3,4 | sum 4,3,4,3,3,3\
                                | merge_dataframes [ | readfile 1,2,3]  \
                                | async name=test_async, [readfile 23,5,4 | collect index='test'] \
                               ] \
                        | table asdf,34,34,key=34 | await name=test_async |  merge_dataframes [ | readfile 1,2,3]"
        top_command_tree = self.get_command_tree_from_otl(test_otl)

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

        async_command_tree = top_command_tree.previous_command_tree_in_pipeline.awaited_command_trees[0]
        self.assertEqual(async_command_tree.computing_node_type, 'SPARK')

        join_command_tree = top_command_tree.first_command_tree_in_pipeline.next_command_tree_in_pipeline
        self.assertEqual(join_command_tree.command.name, 'join')

        merge_dataframes_command_tree = join_command_tree.subsearch_command_trees[0]
        self.assertEqual(merge_dataframes_command_tree.computing_node_type, 'EEP')

    def test_node_job_tree_construction(self):
        test_otl = "| otstats index='test_index' \
                        | join [\
                                | readfile 23,3,4 | sum 4,3,4,3,3,3\
                                | merge_dataframes [ | readfile 1,2,3]  \
                                | async name=test_async, [readfile 23,5,4 | collect index='test'] \
                               ] \
                        | table asdf,34,34,key=34 | await name=test_async |  merge_dataframes [ | readfile 1,2,3]"

        top_command_tree = self.get_command_tree_from_otl(test_otl)


        self.assertEqual(
            top_command_tree.first_command_tree_in_pipeline.command.name,
            'otstats'
        )

        self.assertEqual(
            top_command_tree.first_command_tree_in_pipeline.next_command_tree_in_pipeline.command.name,
            'join'
        )

        # for command_tree in top_command_tree.through_pipeline_iterator():
        #     print(command_tree.command.name)

        all_command_trees = [command_tree for command_tree in top_command_tree.parent_first_order_traverse_iterator('all_child_trees')]

        # print('--all--command--trees')
        # for command_tree in all_command_tree:
        #     print(command_tree.command.name)

        define_computing_node_type_for_command_tree(
            top_command_tree, self.computing_node_type_priority_list, self.command_name_sets
        )

        top_node_job_tree = _construct_node_job_tree(top_command_tree)

        self.check_one_command_tree_in_one_node_job(all_command_trees, top_node_job_tree)

    def test_node_job_tree_construction_with_await_override(self):
        test_otl = "| otstats index='test_index' \
                        | join [\
                                | readfile 23,3,4 | sum 4,3,4,3,3,3\
                                | merge_dataframes [ | readfile 1,2,3]  \
                                | async name=test_async, [readfile 23,5,4 | collect index='test'] \
                               ] \
                        | table asdf,34,34,key=34 | await name=test_async, override=True |  merge_dataframes [ | readfile 1,2,3]"

        top_command_tree = self.get_command_tree_from_otl(test_otl)

        self.assertEqual(
            top_command_tree.first_command_tree_in_pipeline.command.name,
            'readfile'
        )

        self.assertEqual(
            top_command_tree.first_command_tree_in_pipeline.next_command_tree_in_pipeline.command.name,
            'collect'
        )

        # for command_tree in top_command_tree.through_pipeline_iterator():
        #     print(command_tree.command.name)

        all_command_trees = [command_tree for command_tree in top_command_tree.parent_first_order_traverse_iterator('all_child_trees')]

        # print('--all--command--trees')
        # for command_tree in all_command_tree:
        #     print(command_tree.command.name)

        define_computing_node_type_for_command_tree(
            top_command_tree, self.computing_node_type_priority_list, self.command_name_sets
        )

        top_node_job_tree = _construct_node_job_tree(top_command_tree)

        self.check_one_command_tree_in_one_node_job(all_command_trees, top_node_job_tree)

    def test_await_override_false_in_subsearch(self):
        test_otl = "readfile 23,5,4 | async name=test_async, [readfile 23,5,4] | await name=test_async"

        top_command_tree = self.get_command_tree_from_otl(test_otl)
        top_node_job_tree = make_node_job_tree(top_command_tree, subsearch_is_node_job=True)

        # check that every node job has result address
        independent_node_job_trees = top_node_job_tree.leaf_iterator()
        for node_job_tree in top_node_job_tree.children_first_order_traverse_iterator():
            self.assertIsNotNone(node_job_tree.result_address)

        self.assertEqual(top_node_job_tree.result_address.storage_type, ResultStorage.INTERPROCESSING)

    def test_all_read_write_commands_with_address(self):
        test_otl = "| otstats index='test_index' \
                        | join [\
                                | readfile 23,3,4 | sum 4,3,4,3,3,3 | collect index='test_index2'\
                                | merge_dataframes [ | readfile 1,2,3]  \
                                | async name=test_async, [readfile 23,5,4 | collect index='test'] \
                               ] \
                        | table asdf,34,34,key=34 | await name=test_async |  merge_dataframes [ | readfile 1,2,3]"

        top_command_tree = self.get_command_tree_from_otl(test_otl)


        define_computing_node_type_for_command_tree(
            top_command_tree, self.computing_node_type_priority_list, self.command_name_sets
        )

        top_node_job = make_node_job_tree(top_command_tree, self.default_tws, self.default_twf)

        sys_write_result_command_tree = top_node_job.command_tree
        self.assertEqual(sys_write_result_command_tree.command.name, 'sys_write_result')

        for node_job in top_node_job.parent_first_order_traverse_iterator():
            for command_tree in node_job.command_tree.parent_first_order_traverse_iterator():
                if isinstance(command_tree.command, SysReadWriteCommand):
                    self.check_is_it_hash_string(command_tree.command.arguments['path'][0].value)
                    self.assertEqual(command_tree.command.arguments['storage_type'][0].value, ResultStorage.INTERPROCESSING.value)

    def test_read_and_write_has_same_address(self):
        test_otl = "| otstats index='test_index2' | merge_dataframes [readfile 1,2,4]"
        top_node_job = self.get_node_job_tree_from_otl(test_otl)

        read_interproc_command = top_node_job.command_tree.first_command_tree_in_pipeline.command
        write_interproc_command = top_node_job.awaited_node_job_trees[1].command_tree.command

        self.assertEqual(
            read_interproc_command.arguments['path'][0].value,
            write_interproc_command.arguments['path'][0].value
        )

    def test_same_jobs_has_same_hash(self):
        test_otl1 = "| otstats index='test_index2' | merge_dataframes [readfile 1,2,4]"
        top_node_job1 = self.get_node_job_tree_from_otl(test_otl1)

        test_otl2 = "| otstats      index='test_index2' | merge_dataframes     [   readfile 1, 2, 4  ]"
        top_node_job2 = self.get_node_job_tree_from_otl(test_otl2)

        test_otl3 = "| otstats     index='test_index3' | merge_dataframes     [   readfile 1, 2, 4  ]"

        top_node_job3 = self.get_node_job_tree_from_otl(test_otl3)

        self.assertEqual(
            top_node_job1.result_address._path,
            top_node_job2.result_address._path,
        )

        self.assertNotEqual(
            top_node_job1.result_address._path,
            top_node_job3.result_address._path,
        )

    def test_shared_post_processing_result(self):
        test_otl = "| otstats index='test_index2' | merge_dataframes [readfile 1,2,4] | pp_command1 hello"
        top_node_job = self.get_node_job_tree_from_otl(test_otl, shared=True)
        self.assertEqual(top_node_job.result_address.storage_type, ResultStorage.SHARED_POST_PROCESSING.value)

        top_node_job_local = self.get_node_job_tree_from_otl(test_otl, shared=False)
        self.assertEqual(top_node_job_local.result_address.storage_type, ResultStorage.LOCAL_POST_PROCESSING.value)

    def test_read_write_interproc(self):
        test_otl = "| readfile 23,3,4 | sum 4,3,4,3,3,3"
        top_node_job = self.get_node_job_tree_from_otl(test_otl)
        read_file_node_job = top_node_job.children[0]

        read_file_command_tree = read_file_node_job.command_tree.first_command_tree_in_pipeline


        write_interproc_command_tree = read_file_command_tree.next_command_tree_in_pipeline
        # check that first node job has write interproc command
        self.assertEqual(
            read_file_command_tree.command.name,
            'readfile'
        )

        self.assertEqual(
            write_interproc_command_tree.command.name,
            'sys_write_interproc'
        )

    def test_subsearches_in_command_argument(self):
        test_otl = "otstats index=test | join [otstats index=test2 | readfile 1,2,3 | join [readfile 10,20,30]]"
        top_node_job = self.get_node_job_tree_from_otl(test_otl)
        # one job
        command_list = top_node_job.as_command_dict_list()
        first_join_command = command_list[1]
        self.assertEqual(first_join_command['name'], 'join')
        first_subsearch = first_join_command['arguments']['subsearch'][0]
        self.assertEqual(first_subsearch['arg_type'], 'subsearch')
        second_join_command = first_subsearch['value'][2]
        self.assertEqual(second_join_command['name'], 'join')
        last_readfile_command = second_join_command['arguments']['subsearch'][0]['value'][0]
        self.assertEqual(last_readfile_command['name'], 'readfile')

    def test_command_with_subsearch_creates_three_node_job(self):
        test_otl = "| otstats index=test | join [readfile 10,20,30]"
        top_command_tree = self.get_command_tree_from_otl(test_otl)
        top_node_job_tree = make_node_job_tree(top_command_tree, subsearch_is_node_job=True)

        join_command = top_node_job_tree.command_tree.previous_command_tree_in_pipeline.command.name
        self.assertEqual(join_command, 'join')

        readfile_node_job = top_node_job_tree.awaited_node_job_trees[0]
        otstats_node_job = top_node_job_tree.awaited_node_job_trees[1]

        readfile_command = readfile_node_job.command_tree.previous_command_tree_in_pipeline.command.name
        self.assertEqual(readfile_command, 'readfile')

        otstats_command = otstats_node_job.command_tree.previous_command_tree_in_pipeline.command.name
        self.assertEqual(otstats_command, 'otstats')

    def test_cache_two_node_jobs(self):
        test_otl = "readfile 1,2,3 | readfile 3,4,5 | readfile 6,7,8"
        top_node_job = self.get_node_job_tree_from_otl(test_otl)
        # count node jobs
        counter = 0
        for node_job in top_node_job.parent_first_order_traverse_iterator():
            counter += 1
        self.assertEqual(counter, 1, 'Only onw node must be created')

        test_otl_with_set_cache = "readfile 1,2,3 | readfile 3,4,5 | set_cache | readfile 6,7,8"
        top_node_job = self.get_node_job_tree_from_otl(test_otl_with_set_cache)
        counter = 0
        for node_job in top_node_job.parent_first_order_traverse_iterator():
            counter += 1
        self.assertEqual(counter, 2, 'Two node jobs must be created')



