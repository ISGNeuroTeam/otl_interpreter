import sys
import signal
import asyncio
import traceback

from asgiref.sync import sync_to_async
from logging import getLogger
from json import loads


from init_django import init_django
# django initialization should be doen here,
# because handlers import singletons which are initialized at import
init_django()

from message_broker import AsyncConsumer as Consumer

from message_handlers import (
    MessageHandler, ComputingNodeControlHandler, OtlJobHandler, NodeJobStatusHandler,
)

from otl_interpreter.settings import ini_config
from node_job_status_manager import NodeJobStatusManager


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


async def check_job_queue():
    """
    Task to periodically check node job queue
    """
    node_job_status_manager = NodeJobStatusManager()
    time_to_wait = int(ini_config['dispatcher']['check_job_queue_period'])

    while True:
        log.debug('Check node job queue by timeout')
        await node_job_status_manager.check_job_queue()
        await asyncio.sleep(time_to_wait)


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
        asyncio.create_task(
            check_job_queue()
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
