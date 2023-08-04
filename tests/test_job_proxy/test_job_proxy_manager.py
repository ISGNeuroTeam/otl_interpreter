import os
import sys
import time
from datetime import datetime
import subprocess

from pathlib import Path

from django.conf import settings
from otl_interpreter.interpreter_db.models import NodeJob, OtlJob
from base_classes import BaseApiTest, BaseTearDown


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


class TestOtlJobHandler(BaseTearDown, BaseApiTest):
    def setUp(self) -> None:
        BaseApiTest.setUp(self)
        self.base_url = '/ot_simple_rest_job_proxy/v1'
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
        time.sleep(5)

    def test_delete_otl_job_instance_and_run_again(self):
        otl_query = "v2 | otstats index='test_index'"

        data = {
            'original_otl': otl_query,
            'tws': now_timestamp,
            'twf': yesterday_timestamp,
            'cache_ttl': 60,
            'timeout': 0
        }
        response = self.client.post(
            self.full_url('/makejob/'),
            data=data,
            format='json'
        )
        time.sleep(5)
        self.assertEqual(response.status_code, 200)
        OtlJob.objects.all().delete()
        response = self.client.post(
            self.full_url('/makejob/'),
            data=data,
            format='json'
        )
        self.assertEqual(response.status_code, 200)
