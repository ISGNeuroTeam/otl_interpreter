from .command_tree_constructor import construct_command_tree_from_translated_otl_commands




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
            # create weight tree
            # set computing node for every command
            # traverse tree from left to right making node jobs
            # node job is command tree for single node


        # put job trees to database
        # signal to dispatcher




