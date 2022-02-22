import os
import sys
import time
from datetime import datetime
import subprocess

from pathlib import Path

from django.conf import settings
from rest.test import TransactionTestCase
from otl_interpreter.interpreter_db.models import NodeJob
from otl_interpreter.interpreter_db.enums import NodeJobStatus

from base_api_test_class import BaseApiTest

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


class TestNodeJobError(TransactionTestCase, BaseApiTest):
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

        # send request for olt
        data = {
            'otl_query': "| otstats index='test_index' | pp_command2 | readfile 1,2,3 | sum 1,2,3 | pp_command1 test_arg | pp_command2",
            'tws': now_timestamp,
            'twf': yesterday_timestamp
        }

        response = self.client.post(
            self.full_url('/makejob/'),
            data=data,
            format='json'
        )

        time.sleep(20)
        # checking status code
        self.assertEqual(response.status_code, 200)

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

    def tearDown(self):
        self.spark_computing_node.kill()
        self.eep_computing_node.kill()
        self.pp_computing_node.kill()
        self.dispatcher_process.kill()


class TestNodeJobDecline(TransactionTestCase, BaseApiTest):
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

        # send request for olt
        data = {
            'otl_query': "| otstats index='test_index' | join [ | collect index=some_index ] | pp_command2 | readfile 1,2,3 | sum 1,2,3",
            'tws': now_timestamp,
            'twf': yesterday_timestamp
        }
        response = self.client.post(
            self.full_url('/makejob/'),
            data=data,
            format='json'
        )

        if response.status_code == 400:
            print(response.data)

        # checking status code
        self.assertEqual(response.status_code, 200)

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

    def tearDown(self):
        self.spark_computing_node.kill()
        self.eep_computing_node.kill()
        self.pp_computing_node.kill()
        self.dispatcher_process.kill()


class TestNodeResoucesOccupied(TransactionTestCase, BaseApiTest):
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

        # send request for olt
        data = {
            'otl_query': "| otstats index='test_index' ",
            'tws': now_timestamp,
            'twf': yesterday_timestamp
        }
        response = self.client.post(
            self.full_url('/makejob/'),
            data=data,
            format='json'
        )

        # checking status code
        self.assertEqual(response.status_code, 200)

        time.sleep(5)

        spark_node_job = NodeJob.objects.filter(
            computing_node_type='SPARK'
        )[0]

        # spark node has no resources so job moves beetwen three statuses
        self.assertIn(
            spark_node_job.status,
            [
                NodeJobStatus.IN_QUEUE, NodeJobStatus.TAKEN_FROM_QUEUE,
                NodeJobStatus.READY_TO_EXECUTE
            ],
        )

    def tearDown(self):
        self.spark_computing_node.kill()
        self.dispatcher_process.kill()



