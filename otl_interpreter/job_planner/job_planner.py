from otl_interpreter.interpreter_db import node_commands_manager
from .command_tree_constructor import construct_command_tree_from_translated_otl_commands
from .define_computing_node_type_algorithm import define_computing_node_type_for_command_tree


class JobPlanner:
    def __init__(self, node_type_priority):
        self.async_subsearches = {}

    def plan_job(self, translated_otl_commands):
        """
        Gets parsed otl as json
        Splits it into task chains for nodes
        Creates node tasks
        Raises JobPlanException if job planning failed
        :param pasrsed_otl: parsed otl as json
        """
        # create command tree

        command_tree = construct_command_tree_from_translated_otl_commands(translated_otl_commands)
        # split command tree to node job tree

        node_types_priority_list = node_commands_manager.get_node_types_priority()

        command_name_set = {
            node_type: node_commands_manager.get_command_name_set_for_node_type(node_type)
            for node_type in node_types_priority_list
        }

        define_computing_node_type_for_command_tree(
            command_tree, node_types_priority_list, command_name_set
        )

            # traverse tree from left to right making node jobs
            # node job is command tree for single node


        # put job trees to database
        # signal to dispatcher




