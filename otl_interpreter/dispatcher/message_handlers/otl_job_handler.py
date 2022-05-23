import asyncio


from asgiref.sync import sync_to_async

from logging import getLogger
from message_broker import AsyncProducer

from otl_interpreter.interpreter_db import node_job_manager
from otl_interpreter.interpreter_db.enums import NodeJobStatus

from node_job_status_manager import NodeJobStatusManager

from message_serializers.otl_job import OtlJobCommand, OtlJobCommandName, NewOtlJobCommand, CancelOtlJobCommand

from .abstract_message_handler import MessageHandler, Message

log = getLogger('otl_interpreter.dispatcher')


change_node_job_status = sync_to_async(node_job_manager.change_node_job_status)


class OtlJobHandler(MessageHandler):
    def __init__(self):
        self.producer = AsyncProducer()
        self.node_job_status_manager = NodeJobStatusManager()

    async def __aenter__(self):
        await self.producer.start()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        await self.producer.stop()

    async def process_message(self, message: Message) -> None:
        log.debug(f'OtlJobHandler gets node_job{str(message.value)}')
        command_serializer = OtlJobCommand(data=message.value)
        if not command_serializer.is_valid():
            log.error(f'Get invalid otl job command: {command_serializer.error_messages}')
            return

        if command_serializer.validated_data['command_name'] == OtlJobCommandName.NEW_OTL_JOB:
            await self.new_otl_job(command_serializer.validated_data['command'])

        if command_serializer.validated_data['command_name'] == OtlJobCommandName.CANCEL_OTL_JOB:
            await self.cancel_otl_job(command_serializer.validated_data['command'])

    async def new_otl_job(self, new_otl_job_command_dict):
        new_otl_job_command_serializer = NewOtlJobCommand(data=new_otl_job_command_dict)
        if not new_otl_job_command_serializer.is_valid():
            log.error(f'Dispatcher get invalid otl job: {new_otl_job_command_serializer.data}')
            log.error(str(new_otl_job_command_serializer.errors))
            return

        for node_job in new_otl_job_command_serializer.validated_data['node_jobs']:
            await self.node_job_status_manager.change_node_job_status(
                node_job['uuid'], NodeJobStatus.READY_TO_EXECUTE,
                'One of the first node job in otl query',
                node_job
            )

    async def cancel_otl_job(self, cancel_otl_job_command_dict):
        cancel_otl_job_serializer = CancelOtlJobCommand(data=cancel_otl_job_command_dict)
        if not cancel_otl_job_serializer.is_valid():
            log.error(f'Dispatcher get invalid cancel command: {cancel_otl_job_serializer.data}')
            log.error(str(cancel_otl_job_serializer.errors))
            return

        for node_job in cancel_otl_job_serializer.validated_data['node_jobs']:
            await self.node_job_status_manager.change_node_job_status(
                node_job['uuid'], NodeJobStatus.CANCELED,
                'Canceled by user or by timeout',
                node_job
            )
