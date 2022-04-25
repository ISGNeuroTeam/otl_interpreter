import asyncio
import sys
import json
import random

from pprint import pp


from message_broker import AsyncProducer as Producer, AsyncConsumer as Consumer

from .computing_node_config import read_otl_command_syntax, read_computing_node_config


default_config = {
  "uuid": "",
  "computing_node_type": "",
  "host_id": 'local',
  "resources": {
    "job_capacity": 4,
    "cores": 8
  },
  "max_jobs": 10000,             # max node jobs to execute
  "time_on_command": 0.3,
  "lifetime": 180,             # node will be stopped after 60 seconds
  "fail_job": False,           # every job will failed
  "decline_rate": 0,           # evey n-th job will be declined
  "resources_occupied": False  # every resource status with max resources
}


class ComputingNode:
    def __init__(self, computing_node_config, command_syntax):

        self.config = default_config.copy()
        self.config.update(computing_node_config)

        self.command_syntax = command_syntax

        self.consuming_task = None

        self.job_topic = f"{self.config['uuid']}_job"
        self.producer = Producer()
        self.job_counter = 0

        asyncio.create_task(self._send_resources_task())
        asyncio.create_task(self._stop_on_timeout())

    async def start(self):
        await self.producer.start()
        await self._register()
        self.consuming_task = asyncio.create_task(self._start_job_consuming())
        await self.consuming_task

    async def stop(self):
        await self._unregister()
        await self.producer.stop()

    async def _stop_on_timeout(self):
        print(f"shutdown after {self.config['lifetime']}")
        await asyncio.sleep(self.config['lifetime'])
        self.consuming_task.cancel()

    async def _register(self):
        register_command = {
            'computing_node_type': self.config['computing_node_type'],
            'host_id': self.config['host_id'],
            'otl_command_syntax': self.command_syntax,
            'resources': self.config['resources']
        }

        control_command = {
            'computing_node_uuid': self.config['uuid'],
            'command_name': 'REGISTER_COMPUTING_NODE',
            'command': register_command,
        }
        await self.producer.send('computing_node_control', json.dumps(control_command))

    async def _unregister(self):
        unregister_command = {}
        control_command = {
            'computing_node_uuid': self.config['uuid'],
            'command_name': 'UNREGISTER_COMPUTING_NODE',
            'command': unregister_command,
        }
        await self.producer.send('computing_node_control', json.dumps(control_command))

    async def _start_job_consuming(self):
        async with Consumer(self.job_topic, value_deserializer=json.loads) as job_consumer:
            async for job_message in job_consumer:
                #pp(job_message.value)
                asyncio.create_task(self._run_job(job_message.value))

    async def _run_job(self, job):
        decline_rate = self.config['decline_rate']

        # mock computing doesn't calculate anything so just quit
        if job['status'] == 'CANCELED':
            return

        lock = asyncio.Lock()

        job_was_declined = False
        async with lock:
            self.job_counter += 1
            if (decline_rate and self.job_counter % decline_rate == 0) or self.job_counter > self.config['max_jobs']:
                await self._send_node_job_status(
                    job['uuid'],
                    'DECLINED_BY_COMPUTING_NODE',
                    f"Node job was declined",
                )
                job_was_declined = True
                self.job_counter -= 1

        if not self.config['fail_job'] and not job_was_declined:
            await self._send_node_job_status(
                job['uuid'],
                'RUNNING',
                f"Start executing",
            )
            # send status job running
            for command in job['commands']:
                await asyncio.sleep(self.config['time_on_command'])
                # send status command

                await self._send_node_job_status(
                    job['uuid'],
                    'RUNNING',
                    f"command {command['name']} finished",
                    command['name']
                )
            self.job_counter -= 1
            # send status job done
            await self._send_node_job_status(
                job['uuid'],
                'FINISHED',
                f"Node job {job['uuid']} successfully finished",
                job['commands'][-1]['name']
            )
        # send failed
        if self.config['fail_job']:
            await self._send_node_job_status(
                job['uuid'],
                'FAILED',
                f"Node job {job['uuid']} failed",
            )
        await self._send_resources()

    async def _send_node_job_status(
        self, node_job_uuid, status, status_text=None, last_finished_command=None
    ):
        message = {
            'uuid': node_job_uuid,
            'status': status,
            'status_text': status_text,
            'last_finished_command': last_finished_command,
        }
        await self.producer.send('nodejob_status', json.dumps(message))

    async def _send_resources(self):
        if self.config['resources_occupied']:
            cur_resource_usage = self.config['resources']
        else:
            cur_resource_usage = {
                resource: random.randint(0, max_value-1)
                for resource, max_value in self.config['resources'].items()
            }
        resource_command = {
            'resources': cur_resource_usage
        }

        control_command = {
            'computing_node_uuid': self.config['uuid'],
            'command_name': 'RESOURCE_STATUS',
            'command': resource_command,
        }
        await self.producer.send('computing_node_control', json.dumps(control_command))

    async def _send_resources_task(self):
        while True:
            await self._send_resources()
            await asyncio.sleep(5)


async def main():
    computing_node_config_file = sys.argv[1]
    command_syntax_file = sys.argv[2]

    computing_node_config = read_computing_node_config(computing_node_config_file)

    otl_commands_syntax = read_otl_command_syntax(
        command_syntax_file
    )
    computing_node = ComputingNode(computing_node_config, otl_commands_syntax)
    try:
        await computing_node.start()
    except asyncio.CancelledError:
        print('consuming was stopped')
    await computing_node.stop()
