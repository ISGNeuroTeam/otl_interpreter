import sys
import asyncio

from typing import Dict, Type
from logging import getLogger
from json import loads


from init_django import init_django
init_django()

from message_broker import AsyncConsumer as Consumer

from message_handlers import (
    MessageHandler, ComputingNodeControlHandler, OtlJobHandler, NodeJobStatusHandler,
)

log = getLogger('otl_interpreter.dispatcher')

# handlers for topic
topic_handlers_table: Dict[str, Type[MessageHandler]] = {
    'otl_job': OtlJobHandler,
    'nodejob_status': NodeJobStatusHandler,
    'computing_node_control': ComputingNodeControlHandler
}


async def consume_messages(topic):
    async with Consumer(topic, value_deserializer=loads) as consumer:
        async for message in consumer:
            handler_class = topic_handlers_table[topic]
            handler_obj = handler_class()
            await handler_obj.process_message(message)


async def main():
    consume_message_tasks = [
        asyncio.create_task(consume_messages(topic))
        for topic in topic_handlers_table
    ]
    await asyncio.gather(*consume_message_tasks)


if __name__ == "__main__":
    log.info('Dispatcher starting')
    asyncio.run(main())


