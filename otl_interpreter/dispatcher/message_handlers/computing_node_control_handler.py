from logging import getLogger
from asgiref.sync import sync_to_async

from otl_interpreter.interpreter_db import node_commands_manager, NodeCommandsError
from otl_interpreter.settings import host_id as local_host_id


from message_serializers.computing_node_control_command import (
    ComputingNodeControlCommand, ControlNodeCommandName,
    RegisterComputingNodeCommand, UnregisterComputingNodeCommand, ErrorOccuredCommand, ResourceStatusCommand
)
from lock import Lock
from computing_node_pool import computing_node_pool
from node_job_status_manager import NodeJobStatusManager
from .abstract_message_handler import MessageHandler, Message

log = getLogger('otl_interpreter.dispatcher')


# django funcions are not allowed to be used in async mode
register_node = sync_to_async(node_commands_manager.register_node)
register_node_commands = sync_to_async(node_commands_manager.register_node_commands)
get_active_nodes = sync_to_async(node_commands_manager.get_active_nodes_uuids)
node_deactivate = sync_to_async(node_commands_manager.node_deactivate)
node_activate = sync_to_async(node_commands_manager.node_activate)
all_node_uuids = sync_to_async(node_commands_manager.get_all_node_uuids)
get_computing_node_dict = sync_to_async(node_commands_manager.get_computing_node_dict)
get_active_nodes_list = sync_to_async(node_commands_manager.get_active_nodes_list)


class ComputingNodeControlHandler(MessageHandler):
    def __init__(self):
        self.node_job_status_manager = NodeJobStatusManager()
        # if dispatcher was stopped, registered computing nodes might be present in database

    async def __aenter__(self):
        await self._load_computing_nodes_from_db()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        pass

    @staticmethod
    async def _load_computing_nodes_from_db():
        active_nodes = await get_active_nodes_list()
        for node in active_nodes:
            computing_node_pool.add_computing_node(
                node['uuid'], node['type'],
                node['resources'],
                node['host_id'] == local_host_id
            )

    async def process_message(self, message: Message) -> None:
        log.debug(f'computing_node_control recieved message: {message.value}')

        command_serializer = ComputingNodeControlCommand(data=message.value)
        if not command_serializer.is_valid():
            log.error(f'Get invalid computing node control command: {command_serializer.error_messages}')
            return

        computing_node_uuid = command_serializer.validated_data['computing_node_uuid']

        command_name = ControlNodeCommandName(
            command_serializer.validated_data['command_name']
        )

        command_name_method_mapping = {
            ControlNodeCommandName.REGISTER_COMPUTING_NODE: {
                'method': self.process_register,
                'command_class_serializer': RegisterComputingNodeCommand
            },
            ControlNodeCommandName.UNREGISTER_COMPUTING_NODE: {
                'method': self.process_unregister,
                'command_class_serializer': UnregisterComputingNodeCommand
            },
            ControlNodeCommandName.RESOURCE_STATUS: {
                'method': self.process_resource_status,
                'command_class_serializer': ResourceStatusCommand
            },
            ControlNodeCommandName.ERROR_OCCURED: {
                'method': self.process_error_occured,
                'command_class_serializer': ErrorOccuredCommand
            },
        }

        # create concreate command serializer
        concreate_command_serializer = command_name_method_mapping[command_name]['command_class_serializer'](
            data=command_serializer.validated_data['command']
        )

        if not concreate_command_serializer.is_valid():
            log.error(
                f"Computing node control command {command_serializer.validated_data['command_name']} is invalid: \
                {concreate_command_serializer.error_messages} "
            )
            return

        # process command
        await command_name_method_mapping[command_name]['method'](
            computing_node_uuid,
            concreate_command_serializer
        )

    @staticmethod
    async def process_register(computing_node_uuid, register_command: RegisterComputingNodeCommand):

        # only one instance of dispatcher put node information in database
        register_node_lock = Lock(
            key=f'register_computing_node_{computing_node_uuid}'
        )
        if register_node_lock.acquire(blocking=False):
            try:
                await register_node(
                    register_command.validated_data['computing_node_type'],
                    computing_node_uuid,
                    register_command.validated_data['host_id'],
                    register_command.validated_data['resources']
                )
                await register_node_commands(
                    computing_node_uuid,
                    register_command.validated_data['otl_command_syntax']
                )
                register_node_lock.release()
            except NodeCommandsError as err:
                log.error(f'Fail to register computing node {computing_node_uuid}: {str(err)}')

        # add computing node information in computing node pool
        computing_node_pool.add_computing_node(
            computing_node_uuid, register_command.validated_data['computing_node_type'],
            register_command.validated_data['resources'],
            register_command.validated_data['host_id'] == local_host_id
        )

    async def process_error_occured(self, computing_node_uuid, error_occured_command: ErrorOccuredCommand):
        pass

    async def process_resource_status(self, computing_node_uuid, resource_status_command: ResourceStatusCommand):
        resources = resource_status_command.validated_data['resources']
        # if not in computing node pool
        # this happens when dispatcher was reloaded
        if computing_node_uuid not in computing_node_pool:
            # if registered then set active and add to computing node pool
            if computing_node_uuid in await all_node_uuids():
                node_activate(computing_node_uuid)
                computing_node_dict = await get_computing_node_dict(computing_node_uuid)
                computing_node_pool.add_computing_node(
                    computing_node_uuid, computing_node_dict['type'],
                    computing_node_dict['resources'],
                    computing_node_dict['host_id'] == local_host_id
                )
            else:
                log.error(f'Get unregistered computing node resource for computing node: {computing_node_uuid}')
                return

        computing_node_pool.update_node_resources(computing_node_uuid, resources)

    async def process_unregister(self, computing_node_uuid, unregister_command: UnregisterComputingNodeCommand):
        # only one instance of dispatcher put node information in database
        unregister_node_lock = Lock(
            key=f'unregister_computing_node_{computing_node_uuid}'
        )
        if unregister_node_lock.acquire(blocking=False):
            await node_deactivate(computing_node_uuid)
            unregister_node_lock.release()

        computing_node_pool.del_computing_node(computing_node_uuid)
        await self.node_job_status_manager.inactive_computing_node(computing_node_uuid)

    async def check_computing_node_health(self):
        """
        For all inactive computing nodes start unregister process
        """
        inactive_node_uuids = computing_node_pool.get_inactive_node_uuids()
        if inactive_node_uuids:
            log.warning('found inactive node uuids' + str(inactive_node_uuids))

        for inactive_node_uuid in inactive_node_uuids:
            await self.process_unregister(inactive_node_uuid, UnregisterComputingNodeCommand())
