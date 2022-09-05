import os
import sys
import time
from datetime import datetime
import subprocess

from pathlib import Path

from django.conf import settings
from rest.test import TransactionTestCase
from otl_interpreter.interpreter_db.models import NodeJob
from otl_interpreter.interpreter_db.enums import NodeJobStatus, JobStatus
from otl_interpreter.settings import ini_config

from base_classes import BaseApiTest, BaseTearDown

from rest_auth.models import User

plugins_dir = settings.PLUGINS_DIR
base_rest_dir = settings.BASE_DIR

dispatcher_dir = Path(plugins_dir) / 'otl_interpreter' / 'dispatcher'
dispatcher_main = dispatcher_dir / 'main.py'

test_dir = Path(__file__).parent.parent.resolve()


now_timestamp = int(datetime.now().timestamp())
yesterday_timestamp = int(datetime.now().timestamp()) - 60*60*24

dispatcher_proc_env = os.environ.copy()
dispatcher_proc_env["PYTHONPATH"] = f'{base_rest_dir}:{plugins_dir}'

computing_node_env = os.environ.copy()
computing_node_env["PYTHONPATH"] = f'{base_rest_dir}:{plugins_dir}:{str(test_dir)}'


class TestNodeJobError(BaseTearDown, BaseApiTest):
    def setUp(self) -> None:
        BaseApiTest.setUp(self)

        self.dispatcher_process = subprocess.Popen(
            [sys.executable, '-u', dispatcher_main, 'core.settings.test', 'use_test_database'],
            env=dispatcher_proc_env
        )

        # wait until dispatcher start
        time.sleep(5)


        self.spark_computing_node = subprocess.Popen(
            [sys.executable, '-m', 'mock_computing_node', 'spark1.json', 'spark_commands1.json'],
            env=computing_node_env
        )
        self.eep_computing_node = subprocess.Popen(
            [sys.executable, '-m', 'mock_computing_node', 'eep_fail_job.json', 'eep_commands1.json'],
            env=computing_node_env
        )
        self.pp_computing_node = subprocess.Popen(
            [sys.executable, '-m', 'mock_computing_node', 'post_processing1.json', 'post_processing_commands1.json'],
            env=computing_node_env
        )
        # wait until node register
        time.sleep(5)

    def test_job_in_the_middle_failed(self):
        otl_query = "| otstats index='test_index' | pp_command2 | readfile 1,2,3 | sum 1,2,3 | pp_command1 test_arg | pp_command2"
        response = BaseApiTest.make_job_success(self, otl_query)
        time.sleep(20)

        # check that spark node jobs are finished
        spark_node_jobs = NodeJob.objects.filter(computing_node_type='SPARK')
        for node_job in spark_node_jobs:
            self.assertEqual(node_job.status, NodeJobStatus.FINISHED)

        # check that eep node job is failed
        eep_node_jobs = NodeJob.objects.filter(
            computing_node_type='EEP')
        self.assertEqual(len(eep_node_jobs), 1)

        for node_job in eep_node_jobs:
            self.assertEqual(node_job.status, NodeJobStatus.FAILED)

        # check that post processing node jobs is canceled
        finished_pp_node_jobs = NodeJob.objects.filter(
            computing_node_type='POST_PROCESSING',
            status=NodeJobStatus.FINISHED
        )
        self.assertEqual(len(finished_pp_node_jobs), 1)

        canceled_pp_node_jobs = NodeJob.objects.filter(
            computing_node_type='POST_PROCESSING',
            status=NodeJobStatus.CANCELED
        )
        self.assertEqual(len(canceled_pp_node_jobs), 1)


class TestNodeJobDecline(BaseTearDown, BaseApiTest):
    def setUp(self) -> None:
        BaseApiTest.setUp(self)

        self.dispatcher_process = subprocess.Popen(
            [sys.executable, '-u', dispatcher_main, 'core.settings.test', 'use_test_database'],
            env=dispatcher_proc_env
        )

        # wait until dispatcher start
        time.sleep(5)

        self.spark_computing_node = subprocess.Popen(
                [sys.executable, '-m', 'mock_computing_node', 'spark_decline_every_second_job.json', 'spark_commands1.json'],
                env=computing_node_env
        )
        self.eep_computing_node = subprocess.Popen(
            [sys.executable, '-m', 'mock_computing_node', 'eep_decline_every_job.json', 'eep_commands1.json'],
            env=computing_node_env
        )
        self.pp_computing_node = subprocess.Popen(
            [sys.executable, '-m', 'mock_computing_node', 'post_processing1.json', 'post_processing_commands1.json'],
            env=computing_node_env
        )
        # wait until node register
        time.sleep(5)

    def test_node_job_declined(self):
        otl_query = "| otstats index='test_index' | join [ | collect index=some_index ] | pp_command2 | readfile 1,2,3 | sum 1,2,3"
        response = BaseApiTest.make_job_success(self, otl_query)
        time.sleep(65)

        # check that spark all  node jobs are finished
        spark_node_jobs = NodeJob.objects.filter(computing_node_type='SPARK')
        for node_job in spark_node_jobs:
            self.assertEqual(node_job.status, NodeJobStatus.FINISHED)

        # check that post processing node jobs is finished
        pp_node_jobs = NodeJob.objects.filter(
            computing_node_type='POST_PROCESSING',
        )
        for node_job in pp_node_jobs:
            self.assertEqual(node_job.status, NodeJobStatus.FINISHED)

        # check that eep node job is declined, in queue, or taken from queue
        eep_node_jobs = NodeJob.objects.filter(
            computing_node_type='EEP'
        )
        for node_job in eep_node_jobs:
            self.assertIn(
                node_job.status,
                [
                    NodeJobStatus.IN_QUEUE, NodeJobStatus.TAKEN_FROM_QUEUE,
                    NodeJobStatus.DECLINED_BY_COMPUTING_NODE, NodeJobStatus.READY_TO_EXECUTE
                ],
            )


class TestNodeResoucesOccupied(BaseTearDown, BaseApiTest):
    def setUp(self) -> None:
        BaseApiTest.setUp(self)

        self.dispatcher_process = subprocess.Popen(
            [sys.executable, '-u', dispatcher_main, 'core.settings.test', 'use_test_database'],
            env=dispatcher_proc_env
        )

        # wait until dispatcher start
        time.sleep(5)

        self.spark_computing_node = subprocess.Popen(
            [sys.executable, '-m', 'mock_computing_node', 'spark_resources_occupied.json', 'spark_commands1.json'],
            env=computing_node_env
        )
        # wait until node register
        time.sleep(5)

    def test_node_resources_occupied(self):
        # send request for otl
        otl_query = "| otstats index='test_index' "
        response = BaseApiTest.make_job_success(self, otl_query)

        time.sleep(5)

        spark_node_job = NodeJob.objects.filter(
            computing_node_type='SPARK'
        )[0]

        # spark node has no resources so job moves between three statuses
        self.assertIn(
            spark_node_job.status,
            [
                NodeJobStatus.IN_QUEUE, NodeJobStatus.TAKEN_FROM_QUEUE,
                NodeJobStatus.READY_TO_EXECUTE
            ],
        )


class TestNodeReleaseResources(BaseApiTest):
    def setUp(self) -> None:
        BaseApiTest.setUp(self)

        self.dispatcher_process = subprocess.Popen(
            [sys.executable, '-u', dispatcher_main, 'core.settings.test', 'use_test_database'],
            env=dispatcher_proc_env
        )

        # wait until dispatcher start
        time.sleep(5)

        self.spark_computing_node = subprocess.Popen(
            [sys.executable, '-m', 'mock_computing_node', 'spark_max_4_job.json', 'spark_commands1.json'],
            env=computing_node_env
        )
        # wait until node register
        time.sleep(5)

    def tearDown(self):
        self.spark_computing_node.kill()
        self.dispatcher_process.kill()



    # def test_release_resources(self):
    #
    #     job_for_5_sec = "| otstats index='test_index%d' | otstats index='test_index%d2' | otstats index='test_index%d3' | otstats index='test_index%d4' | otstats index='test_index%d7'"
    #     job_for_3_sec = "| otstats index='test_index%d' | otstats index='test_index%d2' | otstats index='test_index%d3'"
    #     job_for_1_sec = "| otstats index='test_index%d'"
    #
    #
    #
    #     jobs_id = [None]*4
    #
    #     # send 4 jobs to occupy all resources
    #     for i in range(4):
    #         response = BaseApiTest.make_job_success(
    #             self, job_for_3_sec % (i, i, i) if i == 3 else job_for_5_sec % (i, i, i, i, i)
    #         )
    #         jobs_id[i] = response['job_id']
    #
    #     # send 1 job and check it's in queue
    #     response = BaseApiTest.make_job_success(self, job_for_1_sec % 1)
    #     time.sleep(2)
    #     job_in_queue = response['job_id']
    #
    #     # checking status code must be still PLANNED
    #     response = self.client.get(
    #         self.full_url(f'/checkjob/?job_id={job_in_queue}'),
    #     )
    #     self.assertEqual(response.status_code, 200)
    #     response_data = response.data
    #     self.assertEqual(response_data['job_status'], JobStatus.PLANNED)
    #
    #     # one job is over by that time
    #     time.sleep(2)
    #
    #     response = self.client.get(
    #         self.full_url(f'/checkjob/?job_id={job_in_queue}'),
    #     )
    #     self.assertEqual(response.status_code, 200)
    #     response_data = response.data
    #     self.assertIn(response_data['job_status'], [JobStatus.RUNNING, JobStatus.FINISHED])
    #
    #     if response_data['job_status'] == JobStatus.RUNNING:
    #         time.sleep(5)
    #         response = self.client.get(
    #             self.full_url(f'/checkjob/?job_id={job_in_queue}'),
    #         )
    #         self.assertEqual(response.status_code, 200)
    #         response_data = response.data
    #         self.assertEqual(response_data['job_status'], JobStatus.FINISHED)


class TestComputingNodeDown(BaseApiTest):
    def setUp(self) -> None:
        BaseApiTest.setUp(self)

        self.dispatcher_process = subprocess.Popen(
            [sys.executable, '-u', dispatcher_main, 'core.settings.test', 'use_test_database'],
            env=dispatcher_proc_env
        )

        # wait until dispatcher start
        time.sleep(5)

        self.spark_computing_node = subprocess.Popen(
            [sys.executable, '-m', 'mock_computing_node', 'spark_1s_command.json', 'spark_commands1.json'],
            env=computing_node_env
        )
        # wait until node register
        time.sleep(5)

    def tearDown(self):
        self.spark_computing_node.kill()
        self.dispatcher_process.kill()


    def test_computing_node_down(self):

        job_for_5_sec = "| otstats index='test_index' | otstats index='test_index2' | otstats index='test_index3' | otstats index='test_index4' | otstats index='test_index7'"
        response = BaseApiTest.make_job_success(self, job_for_5_sec)
        time.sleep(2)
        job_id = response['job_id']

        # checking status code must be still PLANNED
        response = self.client.get(
            self.full_url(f'/checkjob/?job_id={job_id}'),
        )
        if response.status_code != 200:
            print(response)

        self.assertEqual(response.status_code, 200)
        response_data = response.data
        self.assertEqual(response_data['job_status'], JobStatus.RUNNING)

        # kill computing node
        self.spark_computing_node.kill()

        # one job is over by that time
        print(f'waiting ' + ini_config["dispatcher"]["health_check_period"])

        time.sleep(int(ini_config['dispatcher']['health_check_period'])*2)

        response = self.client.get(
            self.full_url(f'/checkjob/?job_id={job_id}'),
        )
        self.assertEqual(response.status_code, 200)
        response_data = response.data
        self.assertEqual(response_data['job_status'], JobStatus.FAILED)


class TestWithOneNode(BaseApiTest):
    def setUp(self) -> None:
        BaseApiTest.setUp(self)

        self.dispatcher_process = subprocess.Popen(
            [sys.executable, '-u', dispatcher_main, 'core.settings.test', 'use_test_database'],
            env=dispatcher_proc_env
        )

        # wait until dispatcher start
        time.sleep(5)

        self.spark_computing_node = subprocess.Popen(
            [sys.executable, '-m', 'mock_computing_node', 'spark1.json', 'spark_commands1.json'],
            env=computing_node_env
        )
        # wait until node register
        time.sleep(5)

    def tearDown(self):
        self.spark_computing_node.kill()
        self.dispatcher_process.kill()


    def test_system_command_usage(self):
        # send request for olt
        otl_query = "| sys_read_interproc asdfasdfasdfasd, 'INTERPROC_STORAGE'"
        response = BaseApiTest.make_job_error(self, otl_query)
        self.assertIn('Translation error', response.data['error'])



