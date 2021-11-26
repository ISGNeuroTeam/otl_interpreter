from logging import getLogger
from .abstract_message_handler import MessageHandler, Message

log = getLogger('otl_interpreter.dispatcher')


class NodeJobStatusHandler(MessageHandler):
    async def process_message(self, message: Message) -> None:
        log.debug(f'nodejob_status_handler recieved message: {message.value}')

