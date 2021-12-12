import asyncio


from asgiref.sync import sync_to_async
from rest_framework.renderers import JSONRenderer

from logging import getLogger
from message_broker import AsyncProducer

from otl_interpreter.interpreter_db import node_job_manager
from otl_interpreter.interpreter_db.enums import NodeJobStatus

from computing_node_pool import computing_node_pool
from message_serializers.otl_job import OtlJobCommand, OtlJobCommandName, NewOtlJobCommand

from .abstract_message_handler import MessageHandler, Message

log = getLogger('otl_interpreter.dispatcher')

set_computing_node_for_node_job = sync_to_async(node_job_manager.set_computing_node_for_node_job)
change_node_job_status = sync_to_async(node_job_manager.change_node_job_status)


class OtlJobHandler(MessageHandler):
    def __init__(self):
        self.producer = AsyncProducer()

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
            log.error(new_otl_job_command_serializer.errors)
            # TODO set node job and otl job error state
            return

        for node_job in new_otl_job_command_serializer.validated_data['node_jobs']:
            computing_node_uuid = computing_node_pool.get_least_loaded_node(node_job['computing_node_type'])
            await set_computing_node_for_node_job(node_job['uuid'].hex, computing_node_uuid)
            await self.producer.send(f"{computing_node_uuid.hex}_job", JSONRenderer().render(node_job))
            await change_node_job_status(node_job['uuid'].hex, NodeJobStatus.SENT_TO_COMPUTING_NODE)
            log.debug('node job sended')

    async def cancel_otl_job(self, otl_job_uuid):
        pass
