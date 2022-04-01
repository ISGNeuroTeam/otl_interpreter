import json
import re
from uuid import UUID
from datetime import datetime
import time
import os
import sys
import subprocess

from pathlib import Path
from django.conf import settings

from otl_interpreter.interpreter_db.enums import ResultStorage, JobStatus, END_STATUSES
from otl_interpreter.interpreter_db.models import OtlJob, NodeJob
from otl_interpreter.interpreter_db import node_commands_manager

from register_test_commands import register_test_commands
from base_api_test_class import BaseApiTest
from mock_computing_node.computing_node_config import read_computing_node_config




plugins_dir = settings.PLUGINS_DIR
base_rest_dir = settings.BASE_DIR

dispatcher_dir = Path(plugins_dir) / 'otl_interpreter' / 'dispatcher'
dispatcher_main = dispatcher_dir / 'main.py'

test_dir = Path(__file__).parent.resolve()

dispatcher_proc_env = os.environ.copy()
dispatcher_proc_env["PYTHONPATH"] = f'{base_rest_dir}:{plugins_dir}'

computing_node_env = os.environ.copy()
computing_node_env["PYTHONPATH"] = f'{base_rest_dir}:{plugins_dir}:{str(test_dir)}'


now_timestamp = int(datetime.now().timestamp())
yesterday_timestamp = int(datetime.now().timestamp()) - 60*60*24


class TestMakeJob(BaseApiTest):
    def setUp(self):
        register_test_commands()
        BaseApiTest.setUp(self)

    def test_makejob_without_errors(self):
        otl_query = "| otstats index='test_index' | join [ | readfile 23,3,4 | sum 4,3,4,3,3,3 | merge_dataframes [ | readfile 1,2,3] | async name=test_async, [readfile 23,5,4 | collect index='test'] ]  | table asdf,34,34,key=34 | await name=test_async, override=True |  merge_dataframes [ | readfile 1,2,3]"
        response = BaseApiTest.make_job_success(self, otl_query)

        # hash string 128 character
        self.assertEqual(len(response['path']), 128)

        self.assertEqual(
            response['storage_type'],
            ResultStorage.INTERPROCESSING.value
        )

    def test_makejob_with_syntax_error(self):
        otl_query = "| otstats2 index='test_index' "
        response = BaseApiTest.make_job_error(self, otl_query)
        self.assertIn('otstats2', response.data['error'])

    def test_makejob_with_job_planner_error(self):
        otl_query = "async name=test, [otstats index='test_index'] | readfile 1,2,3"
        response = BaseApiTest.make_job_error(self, otl_query)
        self.assertEqual('Async subsearches with names: test was never awaited', response.data['error'])


class TestGetJobResult(BaseApiTest):
    def setUp(self):
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
            [sys.executable, '-m', 'mock_computing_node', 'eep1.json', 'eep_commands1.json'],
            env=computing_node_env
        )
        self.pp_computing_node = subprocess.Popen(
            [sys.executable, '-m', 'mock_computing_node', 'post_processing1.json', 'post_processing_commands1.json'],
            env=computing_node_env
        )
        # wait until node register
        time.sleep(5)

    def tearDown(self):
        self.dispatcher_process.kill()
        self.spark_computing_node.kill()
        self.eep_computing_node.kill()
        self.pp_computing_node.kill()

    def test_getresults_no_errors(self):
        otl_query = "| otstats index='test_index' | join [ | collect index=some_index ] | pp_command2 | readfile 1,2,3 | sum 1,2,3"
        response = BaseApiTest.make_job_success(self, otl_query)

        otl_job = OtlJob.objects.get(uuid=response["job_id"])

        for _ in range(15):
            if otl_job.status in [JobStatus.FINISHED, JobStatus.FAILED]:
                break
            time.sleep(1)
            otl_job.refresh_from_db()
        else:
            raise TimeoutError("Job hasn't FINISHED in 15 seconds")

        job_result = self.client.get(
            self.full_url(f'/getresult/?job_id={response["job_id"]}'),
        ).data

        _id = re.match(rf"{ResultStorage.INTERPROCESSING.value}/(\w+)/jsonl/data", job_result['data_urls'][0]).group(1)
        commands = NodeJob.objects.filter(result__path=_id).first().commands
        self.assertEqual(commands[1]["name"], "sum")

    def test_getresults_too_early(self):
        otl_query = "| otstats index='test_index' | join [ | collect index=some_index ] | pp_command1 1 | readfile 1,2,4 | sum 2,2,3"
        response = BaseApiTest.make_job_success(self, otl_query)
        job_result = self.client.get(
            self.full_url(f'/getresult/?job_id={response["job_id"]}'),
        ).data
        self.assertEqual(job_result['status'], 'error')
        self.assertEqual(job_result['error'], 'Results are not ready yet')


class TestNodeWithEmptyResources(BaseApiTest):
    def setUp(self):
        BaseApiTest.setUp(self)

        self.dispatcher_process = subprocess.Popen(
            [sys.executable, '-u', dispatcher_main, 'core.settings.test', 'use_test_database'],
            env=dispatcher_proc_env
        )

        # wait until dispatcher start
        time.sleep(5)

        self.spark_computing_node = subprocess.Popen(
            [sys.executable, '-m', 'mock_computing_node', 'spark_empty_resources.json', 'spark_commands1.json'],
            env=computing_node_env
        )

        # wait until node register
        time.sleep(5)

    def tearDown(self):
        self.spark_computing_node.kill()
        self.dispatcher_process.kill()

    def test_node_register(self):
        guids_list = node_commands_manager.get_active_nodes_uuids()
        self.assertEqual(len(guids_list), 1)
        node_conf = read_computing_node_config('spark_empty_resources.json')
        self.assertEqual(guids_list[0].hex, node_conf['uuid'])

    def test_job_finished(self):
        otl_query = "| otstats index='test_index'"
        response = BaseApiTest.make_job_success(self, otl_query)
        otl_job = OtlJob.objects.get(uuid=response["job_id"])
        for _ in range(15):
            if otl_job.status in [JobStatus.FINISHED, JobStatus.FAILED]:
                break
            time.sleep(1)
            otl_job.refresh_from_db()
        else:
            raise TimeoutError("Job hasn't FINISHED in 15 seconds")


class TestCheckJobView(BaseApiTest):
    def setUp(self):
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
        time.sleep(1)

    def tearDown(self):
        self.dispatcher_process.kill()
        self.spark_computing_node.kill()

    def test_check_job(self):
        otl_query = "| otstats index='test_index' | otstats index='test_index2' | otstats index='test_index3' | otstats index='test_index4' | otstats index='test_index5' | join [ | collect index=some_index | otstats index='test_index6' | otstats index='test_index7'] "
        response = BaseApiTest.make_job_success(self, otl_query)

        job_id = response['job_id']
        otl_job = OtlJob.objects.get(uuid=job_id)

        for _ in range(5):
            if otl_job.status == JobStatus.RUNNING:
                break
            time.sleep(1)
            otl_job.refresh_from_db()
        else:
            raise TimeoutError("Job hasn't achieved running state in 5 seconds")

        response = self.client.get(
            self.full_url(f'/checkjob/?job_id={job_id}'),
        )
        self.assertEqual(response.status_code, 200)
        response_data = response.data
        self.assertEqual(response_data['job_status'], JobStatus.RUNNING)

        for _ in range(10):
            if otl_job.status in [JobStatus.FINISHED, JobStatus.FAILED]:
                break
            time.sleep(1)
            otl_job.refresh_from_db()
        else:
            raise TimeoutError("Job hasn't FINISHED in 10 seconds")

        response = self.client.get(
            self.full_url(f'/checkjob/?job_id={job_id}'),
            format='json'
        )
        self.assertEqual(response.status_code, 200)
        response_data = response.data
        self.assertEqual(response_data['job_status'], JobStatus.FINISHED)


class TestCancelJobView(BaseApiTest):
    def setUp(self):
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

        self.eep_computing_node = subprocess.Popen(
            [sys.executable, '-m', 'mock_computing_node', 'eep1.json', 'eep_commands1.json'],
            env=computing_node_env
        )

        # wait until node register
        time.sleep(1)

    def tearDown(self):
        self.dispatcher_process.kill()
        self.spark_computing_node.kill()
        self.eep_computing_node.kill()

    def test_cancel_job(self):
        otl_query = "| otstats index='test_index' | otstats index='test_index2' | otstats index='test_index3' | otstats index='test_index4' | otstats index='test_index5' | join [ | collect index=some_index | table 1,2,3| otstats index='test_index6' | otstats index='test_index7'] "
        response = BaseApiTest.make_job_success(self, otl_query)

        job_id = response['job_id']
        otl_job = OtlJob.objects.get(uuid=job_id)

        for _ in range(5):
            if otl_job.status == JobStatus.RUNNING:
                break
            time.sleep(1)
            otl_job.refresh_from_db()
        else:
            raise TimeoutError("Job hasn't achieved running state in 5 seconds")

        # check that otl job is running
        time.sleep(2)
        response = self.client.get(
            self.full_url(f'/checkjob/?job_id={job_id}'),
        )
        self.assertEqual(response.status_code, 200)
        response_data = response.data
        self.assertEqual(response_data['job_status'], JobStatus.RUNNING)

        self.cancel_job(job_id)

        for _ in range(10):
            if otl_job.status == JobStatus.CANCELED:
                break
            time.sleep(1)
            otl_job.refresh_from_db()
        else:
            raise TimeoutError("Job hasn't canceled in 10 seconds")

        response = self.client.get(
            self.full_url(f'/checkjob/?job_id={job_id}'),
            format='json'
        )
        self.assertEqual(response.status_code, 200)
        response_data = response.data
        self.assertEqual(response_data['job_status'], JobStatus.CANCELED)

        # check that all node jobs in done state
        for node_job in NodeJob.objects.filter(otl_job=otl_job):
            self.assertIn(node_job.status, END_STATUSES)

