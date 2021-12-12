import sys
import time
from datetime import datetime
import subprocess

from pathlib import Path

from django.conf import settings
from rest.test import TransactionTestCase, APIClient
from otl_interpreter.interpreter_db.models import NodeJob
from otl_interpreter.interpreter_db.enums import NodeJobStatus

from create_test_users import create_test_users

from rest_auth.models import User

plugins_dir = settings.PLUGINS_DIR
base_rest_dir = settings.BASE_DIR

dispatcher_dir = Path(plugins_dir) / 'otl_interpreter' / 'dispatcher'
dispatcher_main = dispatcher_dir / 'main.py'

test_dir = Path(__file__).parent.parent.resolve()


now_timestamp = int(datetime.now().timestamp())
yesterday_timestamp = int(datetime.now().timestamp()) - 60*60*24


class TestOtlJobHandler(TransactionTestCase):
    def setUp(self) -> None:
        create_test_users()
        self.base_url = '/otl_interpreter/v1'
        self.client = APIClient()
        self.user_token = self._get_user_token()
        self.client.credentials(HTTP_AUTHORIZATION='Bearer ' + str(self.user_token))

        time.sleep(5)
        self.dispatcher_process = subprocess.Popen(
            [sys.executable, '-u', dispatcher_main, 'core.settings.test'],
            env={
                'PYTHONPATH': f'{base_rest_dir}:{plugins_dir}'
            }
        )

        # wait until dispatcher start
        time.sleep(5)

        self.spark_computing_node = subprocess.Popen(
            [sys.executable, '-m', 'mock_computing_node', 'spark1.json', 'spark_commands1.json'],
            env={
                'PYTHONPATH': f'{base_rest_dir}:{plugins_dir}:{str(test_dir)}'
            }
        )
        self.eep_computing_node = subprocess.Popen(
            [sys.executable, '-m', 'mock_computing_node', 'eep1.json', 'eep_commands1.json'],
            env={
                'PYTHONPATH': f'{base_rest_dir}:{plugins_dir}:{str(test_dir)}'
            }
        )
        self.pp_computing_node = subprocess.Popen(
            [sys.executable, '-m', 'mock_computing_node', 'post_processing1.json', 'post_processing_commands1.json'],
            env={
                'PYTHONPATH': f'{base_rest_dir}:{plugins_dir}:{str(test_dir)}'
            }
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
        data = {
            'otl_query': "| otstats index='test_index'",
            'tws': now_timestamp,
            'twf': yesterday_timestamp
        }
        response = self.client.post(
            self._full_url('/makejob/'),
            data=data,
            format='json'
        )
        time.sleep(4)
        # checking status code
        self.assertEqual(response.status_code, 200)

        node_job = NodeJob.objects.all().first()
        self.assertEqual(node_job.status, NodeJobStatus.SENT_TO_COMPUTING_NODE)

    def tearDown(self):
        self.spark_computing_node.kill()
        self.eep_computing_node.kill()
        self.pp_computing_node.kill()
        self.dispatcher_process.kill()
