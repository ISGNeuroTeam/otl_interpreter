from logging import getLogger
from asgiref.sync import sync_to_async

from otl_interpreter.interpreter_db import node_commands_manager, NodeCommandsError

from message_serializers.computing_node_control_command import (
    ComputingNodeControlCommand, ControlNodeCommandName,
    RegisterComputingNodeCommand, UnregisterComputingNodeCommand, ErrorOccuredCommand, ResourceStatusCommand
)
from lock import Lock
from computing_node_pool import computing_node_pool

from .abstract_message_handler import MessageHandler, Message

log = getLogger('otl_interpreter.dispatcher')


# django funcions are not allowed to be used in async mode
register_node = sync_to_async(node_commands_manager.register_node)
register_node_commands = sync_to_async(node_commands_manager.register_node_commands)
get_active_nodes = sync_to_async(node_commands_manager.get_active_nodes_uuids)
node_deactivate = sync_to_async(node_commands_manager.node_deactivate)


class ComputingNodeControlHandler(MessageHandler):
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        pass

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
                    register_command.validated_data['resources']
                )
                await register_node_commands(
                    computing_node_uuid,
                    register_command.validated_data['otl_command_syntax']
                )
                register_node_lock.release()
            except NodeCommandsError as err:
                log.error(f'Fail to register computing node {computing_node_uuid}: {str(err)}')

        # add compuging node information in computing node pool
        computing_node_pool.add_computing_node(
            computing_node_uuid, register_command.validated_data['computing_node_type'],
            register_command.validated_data['resources']
        )

    async def process_error_occured(self, computing_node_uuid, error_occured_command: ErrorOccuredCommand):
        pass

    async def process_resource_status(self, computing_node_uuid, resource_status_command: ResourceStatusCommand):
        pass

    @staticmethod
    async def process_unregister(computing_node_uuid, unregister_command: UnregisterComputingNodeCommand):
        # only one instance of dispatcher put node information in database
        unregister_node_lock = Lock(
            key=f'unregister_computing_node_{computing_node_uuid}'
        )
        if unregister_node_lock.acquire(blocking=False):
            await node_deactivate(computing_node_uuid)
            unregister_node_lock.release()

        computing_node_pool.del_computing_node(computing_node_uuid)
