from otl_interpreter.interpreter_db import node_commands_manager
from otl_interpreter.interpreter_db import node_job_manager

from .command_tree_constructor import make_command_tree
from .define_computing_node_type_algorithm import define_computing_node_type_for_command_tree
from .node_job_tree_constructor import make_node_job_tree


class JobPlanner:
    def __init__(self, node_type_priority, subsearch_is_node_job=False):
        self.node_type_priority = node_type_priority
        self.subsearch_is_node_job = subsearch_is_node_job

    def plan_job(self, translated_otl_commands, tws, twf, shared_post_processing=True, subsearch_is_node_job=None):
        """
        Gets translated otl commands
        Splits it into command trees
        Creates node jobs
        Raises JobPlanException if job planning failed
        :param translated_otl_commands: translated_otl_commands
        :param tws: timestamp of window start time as integer
        :param twf: timestamp of window stop time as integer
        :param shared_post_processing: flag, shared result or local for post processing
        :param subsearch_is_node_job: if true each subsearch is separate node job
        """
        subsearch_is_node_job = subsearch_is_node_job or self.subsearch_is_node_job
        # create command tree
        top_command_tree = make_command_tree(
            translated_otl_commands
        )

        # command names by computing node types
        command_name_set = {
            node_type: node_commands_manager.get_command_name_set_for_node_type(node_type)
            for node_type in self.node_type_priority
        }

        define_computing_node_type_for_command_tree(
            top_command_tree, self.node_type_priority, command_name_set
        )

        node_job_tree = make_node_job_tree(top_command_tree, tws, twf, shared_post_processing, subsearch_is_node_job)

        return node_job_tree






