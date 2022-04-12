import os
import sys
import time
from datetime import datetime
import subprocess

from pathlib import Path

from django.conf import settings
from rest.test import TransactionTestCase, APIClient
from otl_interpreter.interpreter_db.models import NodeJob
from otl_interpreter.interpreter_db.enums import NodeJobStatus
from base_api_test_class import BaseApiTest
from create_test_users import create_test_users

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


class TestOtlJobHandler(BaseApiTest):
    def setUp(self) -> None:
        BaseApiTest.setUp(self)

        self.dispatcher_process = subprocess.Popen(
            [sys.executable, '-u', dispatcher_main, 'core.settings.test'],
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

    def _get_user_token(self):
        data = {
            "login": "ordinary_user1",
            "password": "ordinary_user1"
        }
        response = self.client.post('/auth/login/', data=data)
        return response.data['token']

    def _full_url(self, url):
        return self.base_url + url

    def test_node_jobs_done(self):
        # send request for olt
        otl_query = "| otstats index='test_index'| sum 1,2,3 | pp_command1 test"
        response = BaseApiTest.make_job_success(self, otl_query)
        time.sleep(10)

        node_jobs = NodeJob.objects.all()
        self.assertEqual(len(node_jobs), 3)
        for node_job in node_jobs:
            self.assertEqual(
                node_job.result.status, 'CALCULATED'
            )
            self.assertEqual(node_job.status, NodeJobStatus.FINISHED)

    def tearDown(self):
        self.dispatcher_process.kill()
        self.spark_computing_node.kill()
        self.eep_computing_node.kill()
        self.pp_computing_node.kill()


