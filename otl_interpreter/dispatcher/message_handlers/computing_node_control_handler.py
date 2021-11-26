from logging import getLogger
from asgiref.sync import sync_to_async

from otl_interpreter.interpreter_db import node_commands_manager, NodeCommandsError

from message_serializers.computing_node_control_command import (
    ComputingNodeControlCommand, ControlNodeCommandName,
    RegisterComputingNodeCommand, UnregisterComputingNodeCommand, ErrorOccuredCommand, ResourceStatusCommand
)
from .abstract_message_handler import MessageHandler, Message

log = getLogger('otl_interpreter.dispatcher')


# django funcions are not allowed to be used in async mode
register_node = sync_to_async(node_commands_manager.register_node)
register_node_commands = sync_to_async(node_commands_manager.register_node_commands)
get_active_nodes = sync_to_async(node_commands_manager.get_active_nodes_guids)


class ComputingNodeControlHandler(MessageHandler):

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
        try:
            await register_node(
                register_command.validated_data['computing_node_type'],
                computing_node_uuid
            )
            await register_node_commands(
                computing_node_uuid,
                register_command.validated_data['otl_command_syntax']
            )

            nodes = await get_active_nodes()

        except NodeCommandsError as err:
            log.error(f'Fail to register computing node {computing_node_uuid}: {str(err)}')

        # todo register resources

    async def process_error_occured(self, computing_node_uuid, error_occured_command: ErrorOccuredCommand):
        pass

    async def process_resource_status(self, computing_node_uuid, resource_status_command: ResourceStatusCommand):
        pass

    def process_unregister(self, computing_node_uuid, unregister_command: UnregisterComputingNodeCommand):
        pass

