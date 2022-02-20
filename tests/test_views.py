import json
import re
from uuid import UUID
from datetime import datetime
import time
import os
import sys
import subprocess

from rest.test import TransactionTestCase as TestCase, APIClient

from otl_interpreter.interpreter_db.enums import ResultStorage, JobStatus
from otl_interpreter.interpreter_db.models import OtlJob, NodeJob
from create_test_users import create_test_users
from register_test_commands import register_test_commands


from pathlib import Path

from django.conf import settings


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


class ViewTestCase(TestCase):
    base_url = '/otl_interpreter/v1'

    def _get_user_token(self):
        data = {
            "login": "ordinary_user1",
            "password": "ordinary_user1"
        }
        response = self.client.post('/auth/login/', data=data)
        return response.data['token']

    @staticmethod
    def _full_url(url):
        return ViewTestCase.base_url + url


class TestMakeJob(ViewTestCase):
    def setUp(self):
        register_test_commands()
        create_test_users()
        self.client = APIClient()
        self.user_token = self._get_user_token()
        self.client.credentials(HTTP_AUTHORIZATION='Bearer ' + str(self.user_token))

    def test_makejob_without_errors(self):
        data = {
            'otl_query': "| otstats index='test_index' | join [ | readfile 23,3,4 | sum 4,3,4,3,3,3 | merge_dataframes [ | readfile 1,2,3] | async name=test_async, [readfile 23,5,4 | collect index='test'] ]  | table asdf,34,34,key=34 | await name=test_async, override=True |  merge_dataframes [ | readfile 1,2,3]",
            'tws': now_timestamp,
            'twf': yesterday_timestamp
        }
        response = self.client.post(
            self._full_url('/makejob/'),
            data=data,
            format='json'
        )

        # checking status code
        self.assertEqual(response.status_code, 200)

        self.assertEqual(response.data['status'], 'success')

        self.assertEqual(len(response.data['job_id']), 32)

        # hash string 128 character
        self.assertEqual(len(response.data['path']), 128)

        self.assertEqual(
            response.data['storage_type'],
            ResultStorage.INTERPROCESSING.value
        )

    def test_makejob_with_syntax_error(self):
        data = {
            'otl_query': "| otstats2 index='test_index' ",
            'tws': now_timestamp,
            'twf': yesterday_timestamp
        }
        response = self.client.post(
            self._full_url('/makejob/'),
            data=data
        )

        self.assertEqual(response.status_code, 400)

        self.assertEqual(response.data['status'], 'error')
        self.assertIn('otstats2', response.data['error'])

    def test_makejob_with_job_planner_error(self):
        data = {
            'otl_query': "async name=test, [otstats index='test_index'] | readfile 1,2,3",
            'tws': now_timestamp,
            'twf': yesterday_timestamp
        }
        response = self.client.post(
            self._full_url('/makejob/'),
            data=data
        )

        self.assertEqual(response.status_code, 400)

        self.assertEqual(response.data['status'], 'error')
        self.assertEqual('Async subsearches with names: test was never awaited', response.data['error'])


class TestGetJobResult(ViewTestCase):
    def setUp(self):
        create_test_users()
        self.client = APIClient()
        self.user_token = self._get_user_token()
        self.client.credentials(HTTP_AUTHORIZATION='Bearer ' + str(self.user_token))

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

    def test_getresults_no_errors(self):
        data = {
            'otl_query': "| otstats index='test_index' | join [ | collect index=some_index ] | pp_command2 | readfile 1,2,3 | sum 1,2,3",
            'tws': now_timestamp,
            'twf': yesterday_timestamp
        }
        response = self.client.post(
            self._full_url('/makejob/'),
            data=data,
            format='json'
        ).data
        otl_job = OtlJob.objects.get(uuid=response["job_id"])

        for _ in range(15):
            if otl_job.status in [JobStatus.FINISHED, JobStatus.FAILED]:
                break
            time.sleep(1)
            otl_job.refresh_from_db()
        else:
            raise TimeoutError("Job hasn't FINISHED in 15 seconds")

        job_result = self.client.get(
            self._full_url(f'/getresult/?job_id={response["job_id"]}'),
        ).data

        _id = re.match(r"INTERPROC_STORAGE/(\w+)/jsonl/data", job_result['data_urls'][0]).group(1)
        commands = NodeJob.objects.filter(result__path=_id).first().commands
        self.assertEqual(commands[1]["name"], "sum")

    def test_getresults_too_early(self):
        data = {
            'otl_query': "| otstats index='test_index' | join [ | collect index=some_index ] | pp_command1 1 | readfile 1,2,4 | sum 2,2,3",
            'tws': now_timestamp,
            'twf': yesterday_timestamp
        }
        response = self.client.post(
            self._full_url('/makejob/'),
            data=data,
            format='json'
        ).data

        job_result = self.client.get(
            self._full_url(f'/getresult/?job_id={response["job_id"]}'),
        ).data
        self.assertEqual(job_result['status'], 'failure')
        self.assertEqual(job_result['error_message'], 'Results are not ready yet')

    def tearDown(self):
        self.spark_computing_node.terminate()
        self.eep_computing_node.terminate()
        self.pp_computing_node.terminate()
        self.dispatcher_process.terminate()
