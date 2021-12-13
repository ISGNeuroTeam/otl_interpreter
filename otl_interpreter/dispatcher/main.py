import sys
import signal
import asyncio
import traceback

from asgiref.sync import sync_to_async
from logging import getLogger
from json import loads


from init_django import init_django
init_django()

from message_broker import AsyncConsumer as Consumer

from message_handlers import (
    MessageHandler, ComputingNodeControlHandler, OtlJobHandler, NodeJobStatusHandler,
)

log = getLogger('otl_interpreter.dispatcher')


async def consume_messages(topic, handler_class, consumer_extra_config=None):
    async with handler_class() as message_handler:
        async with Consumer(topic, value_deserializer=loads, extra_config=consumer_extra_config) as consumer:
            async for message in consumer:
                try:
                    await message_handler.process_message(message)
                except Exception as err:
                    log.error(f'Error occured while process message {message.value}')
                    tb = traceback.format_exc()
                    log.error(tb)
                    print(tb)


async def main():
    tasks = [
        asyncio.create_task(
            consume_messages('computing_node_control', ComputingNodeControlHandler, {'broadcast': True})
        ),
        asyncio.create_task(
            consume_messages('nodejob_status', NodeJobStatusHandler)
        ),
        asyncio.create_task(
            consume_messages('otl_job', OtlJobHandler)
        ),
    ]
    await asyncio.gather(*tasks)


# to ensure that try / finaly block will be executed and __exit__ methods of contex manager
def terminate_handler(signum, frame):
    log.info('Dispatcher shutdown')
    sys.exit(0)


signal.signal(signal.SIGTERM, terminate_handler)


if __name__ == "__main__":
    log.info('Dispatcher starting')
    asyncio.run(main())
