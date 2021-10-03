from uuid import uuid4
from otl_interpreter.interpreter_db import node_commands_manager

from .command_tree_constructor import construct_command_tree_from_translated_otl_commands
from .define_computing_node_type_algorithm import define_computing_node_type_for_command_tree
from .command import Command, Arg, ArgType
from .command_tree import CommandTree


class JobPlanner:
    def __init__(self, node_type_priority):
        self.node_type_priority = node_type_priority

    def plan_job(self, translated_otl_commands):
        """
        Gets translated otl commands
        Splits it into command trees
        Creates node jobs
        Raises JobPlanException if job planning failed
        :param translated_otl_commands: translated_otl_commands
        """
        # create command tree
        command_tree, awaited_command_trees = construct_command_tree_from_translated_otl_commands(translated_otl_commands)

        # get tree with sys_write_result on the top
        top_command_tree = self._make_top_command_tree(
            command_tree, awaited_command_trees
        )

        node_types_priority_list = node_commands_manager.get_node_types_priority()

        command_name_set = {
            node_type: node_commands_manager.get_command_name_set_for_node_type(node_type)
            for node_type in node_types_priority_list
        }

        define_computing_node_type_for_command_tree(
            top_command_tree, node_types_priority_list, command_name_set
        )

    def _make_top_command_tree(self, command_tree, awaited_command_trees):
        sys_write_result_command = self._make_sys_write_result_command_tree()

        return CommandTree(
            sys_write_result_command,
            previous_command_tree_in_pipeline=command_tree,
            awaited_command_trees=awaited_command_trees
        )

    @staticmethod
    def _make_sys_write_result_command_tree():
        """
        :return:
        command tree with  sys_write_result command for top of the tree
        """
        return Command(
            name='sys_write_result',
            args=[
                Arg(
                    arg_type=ArgType.NAMED,
                    arg_data_type='string',
                    arg_value=uuid4().hex,
                    arg_name='address',

                )
            ]
        )

