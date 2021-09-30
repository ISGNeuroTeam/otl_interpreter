from otl_interpreter.settings import ini_config
from .models import NodeCommand, ComputingNode, CommandType


class NodeCommandsError(ValueError):
    pass

class NodeCommandsManager:

    @staticmethod
    def register_node(node_type, node_guid):
        """
        creates node if it doesn't exist
        :param node_type: type string, spark, eep or post_processing
        :param node_guid: node global id, hex string
        :return:
        """
        try:
            computing_node = ComputingNode.objects.get(guid=node_guid)
        except ComputingNode.DoesNotExist:
            computing_node = ComputingNode(type=node_type, guid=node_guid)
        computing_node.active = True
        computing_node.save()

    @staticmethod
    def node_deactivate(node_guid):
        """
        make node and all node commands inactive
        :param node_guid:
        :return:
        """
        computing_node = ComputingNode.objects.get(guid=node_guid)
        computing_node.active = False
        computing_node.save()

    @staticmethod
    def node_activate(node_guid):
        """
        make node and all node commands active
        :param node_guid:
        :return:
        """
        computing_node = ComputingNode.objects.get(guid=node_guid)
        computing_node.active = True
        computing_node.save()

    @staticmethod
    def register_required_commands(commands):
        """
        Register required commands. All computing nodes must have those commands. Rases NodeCommandsError if get invalid command syntax
        :param commands: json array with field 'command name':  command syntax
        """
        node_commands = [
            NodeCommand(node=None, name=command_name, syntax=command_syntax, type=CommandType.REQUIRED_COMMAND)
            for command_name, command_syntax in commands.items()
        ]

        NodeCommand.objects.bulk_create(node_commands)

    @staticmethod
    def register_node_commands(node_guid, commands):

        """
        Register new commands on node. Rases NodeCommandsError if get invalid command syntax
        :param node_guid: node guid. If node_guid
        :param commands: json array with field 'command name':  command syntax
        """


        try:
            computin_node = ComputingNode.objects.get(guid=node_guid)
        except ComputingNode.DoesNotExist:
            raise NodeCommandsError(f'Node with guid {node_guid} was not registered')

        node_commands = [
            NodeCommand(node=computin_node, name=command_name, syntax=command_syntax)
            for command_name, command_syntax in commands.items()
        ]

        # todo check command syntax
        # if command with name already exist syntax must be the same
        # todo check commands set of node type all nodes with save type must have the same command set

        NodeCommand.objects.bulk_create(node_commands)

    @staticmethod
    def get_node_types():
        """
        Returns node types set
        """
        return set(ComputingNode.objects.values_list('type', flat=True).distinct())

    @staticmethod
    def get_node_types_priority():
        """
        :return:
        list of node types in priority order
        """
        return ini_config['job_planer']['computing_node_type_priority'].split()

    @staticmethod
    def get_command_name_set_for_node_type(node_type):
        """
        Returns set of commands for given node type
        :param node_type:
        :return:
        Set of command names
        """
        return set(
            NodeCommand.objects.filter(node__type__exact=node_type).values_list('name', flat=True)
        ).union(
           NodeCommandsManager.get_command_name_set_available_on_all_nodes()
        )

    @staticmethod
    def get_command_name_set_available_on_all_nodes():
        """
        Returns set of commands available for all computing nodes
        """
        return set(NodeCommand.objects.filter(node__type=None).values_list('name', flat=True))

    @staticmethod
    def get_commands_syntax():
        """
        Returns all commands as dictionary. keys - commands names, value - syntax
        """
        return dict(NodeCommand.objects.filter(active=True).values_list('name', 'syntax'))



