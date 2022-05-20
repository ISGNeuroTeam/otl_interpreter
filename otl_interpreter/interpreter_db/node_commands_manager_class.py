from datetime import datetime
from functools import wraps
from uuid import UUID
from django.forms.models import model_to_dict

from otl_interpreter.settings import get_cache
from otl_interpreter.interpreter_db.models import NodeCommand, ComputingNode

from otl_interpreter.core_commands import job_planer_commands, sys_computing_node_commands


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
        return self._cache.get('commands_updated_timestamp', datetime.min)

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

    def register_node(self, node_type, node_uuid, host_id, resources=None):
        """
        creates node if it doesn't exist
        :param node_type: type string, spark, eep or post_processing
        :param node_uuid: node global id, hex string
        :poram host_id: host id, any unique string to identify host
        :param resources: dictionary with node resources,
        keys - arbituarary string
        value - any positive integer
        :return:
        """
        if resources is None:
            resources = dict()
        try:
            computing_node = ComputingNode.objects.get(uuid=node_uuid)
        except ComputingNode.DoesNotExist:
            computing_node = ComputingNode(
                type=node_type, uuid=node_uuid, resources=resources, host_id=host_id
            )
        computing_node.active = True
        computing_node.save()

    @_set_commands_updated_timestamp_decorator
    def node_deactivate(self, node_uuid):
        """
        make node and all node commands inactive
        :param node_uuid:
        :return:
        """
        computing_node = ComputingNode.objects.get(uuid=node_uuid)
        computing_node.active = False
        computing_node.save()
        computing_node.node_commands.update(active=False)

    @_set_commands_updated_timestamp_decorator
    def node_activate(self, node_uuid):
        """
        make node and all node commands active
        :param node_uuid:
        :return:
        """
        computing_node = ComputingNode.objects.get(uuid=node_uuid)
        computing_node.active = True
        computing_node.save()
        computing_node.node_commands.update(active=True)

    @staticmethod
    def get_active_nodes_uuids(computing_node_type=None):
        if computing_node_type is None:
            return list(
                ComputingNode.objects.filter(active=True).values_list('uuid', flat=True)
            )
        else:
            return list(
                ComputingNode.objects.filter(
                    active=True, type=computing_node_type
                ).value_list('uuid', flat=True)
            )

    @staticmethod
    def get_active_nodes_list():
        """
        Returns list of dictionaries representing active computing nodes
        """
        return list(
            map(
                model_to_dict,
                ComputingNode.objects.filter(active=True)
            )
        )

    @staticmethod
    def get_all_node_uuids():
        return list(
            ComputingNode.objects.values_list('uuid', flat=True)
        )

    @_set_commands_updated_timestamp_decorator
    def register_node_commands(self, node_uuid, commands):

        """
        Register new commands on node. Rases NodeCommandsError if get invalid command syntax
        :param node_uuid: node uuid. If node_uuid
        :param commands: json array with field 'command name':  command syntax
        """

        try:
            computing_node = ComputingNode.objects.get(uuid=node_uuid)
        except ComputingNode.DoesNotExist:
            raise NodeCommandsError(f'Node with uuid {node_uuid} was not registered')

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
        return set(ComputingNode.objects.filter(active=True).values_list('type', flat=True).distinct())

    @staticmethod
    def get_command_name_set_for_node_type(node_type):
        """
        Returns set of commands for given node type
        :param node_type:
        :return:
        Set of command names
        """
        return set(
            NodeCommand.objects.filter(node__type__exact=node_type, active=True).values_list('name', flat=True)
        ).union(
           NodeCommandsManager.get_command_name_set_available_on_all_nodes()
        )

    @staticmethod
    def get_command_name_set_available_on_all_nodes():
        """
        Returns set of commands available for all computing nodes
        """
        return set(
            NodeCommand.objects.filter(node=None).values_list('name', flat=True)
        ).union(
            set(sys_computing_node_commands.keys())
        )

    @staticmethod
    def get_commands_syntax():
        """
        Returns all commands as dictionary. keys - commands names, value - syntax
        """
        result = dict(
            NodeCommand.objects.filter(active=True).values_list('name', 'syntax')
        )
        result.update(job_planer_commands)
        # ignore commands with sys_ prefix
        # computing node must not be register commands with sys_ prefix. Those commands considered as system commands
        result = {key: value for key, value in result.items() if not key.startswith('sys_')}
        return result

    @staticmethod
    def get_computing_node_dict(computing_node_uuid: UUID):
        """
        Returns computing node info as dict
        """
        try:
            node = ComputingNode.objects.get(uuid=computing_node_uuid)
            return model_to_dict(node)
        except ComputingNode.DoesNotExist:
            return {}
