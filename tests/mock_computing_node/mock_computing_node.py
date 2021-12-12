import sys
import json
from pprint import pp


from message_broker import AsyncProducer as Producer, AsyncConsumer as Consumer

from .computing_node_config import read_otl_command_syntax, read_computing_node_config


class ComputingNode:
    def __init__(self, computing_node_config, command_syntax):
        self.config = computing_node_config
        self.command_syntax = command_syntax

        self.job_topic = f"{self.config['uuid']}_job"
        self.producer = Producer()

    async def start(self):
        await self.producer.start()
        await self._register()
        await self._start_job_consuming()

    async def stop(self):
        await self.producer.stop()

    async def _register(self):
        register_command = {
            'computing_node_type': self.config['computing_node_type'],
            'otl_command_syntax': self.command_syntax,
            'resources': self.config['resources']
        }

        control_command = {
            'computing_node_uuid': self.config['uuid'],
            'command_name': 'REGISTER_COMPUTING_NODE',
            'command': register_command,
        }
        await self.producer.send('computing_node_control', json.dumps(control_command))

    async def _start_job_consuming(self):
        async with Consumer(self.job_topic, value_deserializer=json.loads) as job_consumer:
            async for job_message in job_consumer:
                pp(job_message.value)


async def main():
    computing_node_config_file = sys.argv[1]
    command_syntax_file = sys.argv[2]

    computing_node_config = read_computing_node_config(computing_node_config_file)

    otl_commands_syntax = read_otl_command_syntax(
        command_syntax_file
    )
    computing_node = ComputingNode(computing_node_config, otl_commands_syntax)
    await computing_node.start()
    await computing_node.stop()
