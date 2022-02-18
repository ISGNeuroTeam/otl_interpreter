import json
from uuid import UUID
from datetime import datetime
import time

from rest.test import TestCase, APIClient

from otl_interpreter.interpreter_db.enums import ResultStorage, JobStatus
from otl_interpreter.interpreter_db.models import OtlJob
from create_test_users import create_test_users
from register_test_commands import register_test_commands

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
        register_test_commands()
        create_test_users()
        self.client = APIClient()
        self.user_token = self._get_user_token()
        self.client.credentials(HTTP_AUTHORIZATION='Bearer ' + str(self.user_token))

        self.data = {
            'otl_query': "| otstats index='test_index' | join [ | readfile 23,3,4 | sum 4,3,4,3,3,3 | merge_dataframes [ | readfile 1,2,3] | async name=test_async, [readfile 23,5,4 | collect index='test'] ]  | table asdf,34,34,key=34 | await name=test_async, override=True |  merge_dataframes [ | readfile 1,2,3]",
            'tws': now_timestamp,
            'twf': yesterday_timestamp
        }
        self.response = self.client.post(
            self._full_url('/makejob/'),
            data=self.data,
            format='json'
        ).data

    def test_getresults_no_errors(self):
        otl_job = OtlJob.objects.get(uuid=self.response["job_id"])
        while otl_job.status not in [JobStatus.FINISHED, JobStatus.FAILED]:
            time.sleep(1)
            otl_job.refresh_from_db()
            print("Heartbeat...")

        response = self.client.get(
            self._full_url(f'/getresult/?job_id={self.response["job_id"]}'),
        )
        print(f"RESPONSE!!!: {response.data}")
        self.assertTrue(False)

    def tearDown(self):
        self.computing_node_process.kill()
        self.dispatcher_process.kill()