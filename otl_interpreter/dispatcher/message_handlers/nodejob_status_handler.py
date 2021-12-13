from logging import getLogger
from .abstract_message_handler import MessageHandler, Message

from otl_interpreter.interpreter_db import node_job_manager
from otl_interpreter.interpreter_db.enums import NodeJobStatus

from message_serializers.nodejob_status import NodeJobStatusSerializer

from node_job_status_manager import NodeJobStatusManager


log = getLogger('otl_interpreter.dispatcher')


class NodeJobStatusHandler(MessageHandler):
    def __init__(self):
        self.node_job_status_manager = NodeJobStatusManager()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        pass

    async def process_message(self, message: Message) -> None:
        nodejob_status_serializer = NodeJobStatusSerializer(data=message.value)
        if not nodejob_status_serializer.is_valid():
            log.error(f'Get invalid nodejob status: {nodejob_status_serializer.errors}')
            # TODO process invalid message case
            return

        await self.node_job_status_manager.change_node_job_status(
            nodejob_status_serializer.validated_data['uuid'],
            nodejob_status_serializer.validated_data['status'],
            nodejob_status_serializer.validated_data['status_text'],
        )



