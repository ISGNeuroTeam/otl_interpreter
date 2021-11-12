from datetime import datetime
from functools import wraps
from otl_interpreter.settings import get_cache
from otl_interpreter.interpreter_db.models import NodeCommand, ComputingNode, CommandType


class NodeCommandsError(ValueError):
    pass


class NodeCommandsManager:

    def __init__(self, cache=None):
        """
        :param cache: cache object  wiht set and get methods for internal usage
        """
        if cache is None:
            self._cache = get_cache()
        else:
            self._cache = cache

    @property
    def commands_updated_timestamp(self):
        return self._cache.get('commands_updated_timestamp')

    @commands_updated_timestamp.setter
    def commands_updated_timestamp(self, value=None):
        value = value or datetime.now()
        self._cache.set('commands_updated_timestamp', value, timeout=None)

    def _set_commands_updated_timestamp_decorator(f):
        """
        Decorator for inner methods
        Sets  command_updated_timestamp property to now()
        """
        @wraps(f)
        def wrap_function(self, *args, **kwargs):
            result = f(self, *args, **kwargs)
            self.commands_updated_timestamp = datetime.now()
            return result

        return wrap_function

    def commands_were_updated(self, timestamp: datetime):
        """
        Returns true if command were updated after given timestamp
        """
        return self.commands_updated_timestamp > timestamp

    def register_node(self, node_type, node_guid):
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

    @_set_commands_updated_timestamp_decorator
    def node_deactivate(self, node_guid):
        """
        make node and all node commands inactive
        :param node_guid:
        :return:
        """
        computing_node = ComputingNode.objects.get(guid=node_guid)
        computing_node.active = False
        computing_node.save()
        computing_node.node_commands.update(active=False)

    @_set_commands_updated_timestamp_decorator
    def node_activate(self, node_guid):
        """
        make node and all node commands active
        :param node_guid:
        :return:
        """
        computing_node = ComputingNode.objects.get(guid=node_guid)
        computing_node.active = True
        computing_node.save()
        computing_node.node_commands.update(active=True)

    @staticmethod
    def register_required_commands(commands):
        """
        Register required commands. All computing nodes must have those commands.
        :param commands: json array with field 'command name':  command syntax
        """
        node_commands = [
            NodeCommand(node=None, name=command_name, syntax=command_syntax, type=CommandType.REQUIRED_COMMAND)
            for command_name, command_syntax in commands.items()
        ]

        NodeCommand.objects.bulk_create(node_commands)

    @_set_commands_updated_timestamp_decorator
    def register_node_commands(self, node_guid, commands):

        """
        Register new commands on node. Rases NodeCommandsError if get invalid command syntax
        :param node_guid: node guid. If node_guid
        :param commands: json array with field 'command name':  command syntax
        """

        try:
            computing_node = ComputingNode.objects.get(guid=node_guid)
        except ComputingNode.DoesNotExist:
            raise NodeCommandsError(f'Node with guid {node_guid} was not registered')

        for command_name, command_syntax in commands.items():
            NodeCommand.objects.update_or_create(
                defaults={
                    'syntax': command_syntax,
                    'active': True,
                },
                node=computing_node,
                name=command_name,
            )

    @staticmethod
    def is_command_need_timewindow(command_name):
        """
        Returns true if command need earliest and latest timestpaps arguments to be added
        """
        # todo optimize this, without database
        command = NodeCommand.objects.filter(name=command_name).first()
        if command:
            return command.syntax.get('use_timewindow', False)
        return False

    @staticmethod
    def get_node_types():
        """
        Returns node types set
        """
        return set(ComputingNode.objects.values_list('type', flat=True).distinct())

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
