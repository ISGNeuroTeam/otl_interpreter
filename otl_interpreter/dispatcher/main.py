import sys
import asyncio

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
    async with Consumer(topic, value_deserializer=loads, extra_config=consumer_extra_config) as consumer:
        async for message in consumer:
            handler_obj = handler_class()
            await handler_obj.process_message(message)


async def main():
    consume_message_tasks = [
        asyncio.create_task(
            consume_messages('computing_node_control', ComputingNodeControlHandler, {'broadcast': True})
        ),
        asyncio.create_task(
            consume_messages('nodejob_status', NodeJobStatusHandler)
        ),
        asyncio.create_task(
            consume_messages('otl_job', OtlJobHandler)
        )
    ]
    await asyncio.gather(*consume_message_tasks)


if __name__ == "__main__":
    log.info('Dispatcher starting')
    asyncio.run(main())


