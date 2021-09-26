
class Command:
    def __init__(self, tranlated_command, computin_node=None):
        """
        :param tranlated_command parsed otl json without subsearches and await, async
        :param computin_node: node guid
        """
        self.translated_command = tranlated_command
        self.computing_node = None

        if computin_node:
            self.set_computing_node(computin_node)

    def set_computing_node(self, computing_node):
        self.computing_node = computing_node





