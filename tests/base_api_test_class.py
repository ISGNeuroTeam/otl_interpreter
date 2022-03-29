from datetime import datetime
from rest.test import APIClient, TransactionTestCase as TestCase
from create_test_users import create_test_users


class BaseApiTest(TestCase):
    def setUp(self) -> None:
        create_test_users()
        self.base_url = '/otl_interpreter/v1'
        self.client = APIClient()
        self.user_token = self._get_user_token()
        self.client.credentials(HTTP_AUTHORIZATION='Bearer ' + str(self.user_token))

    def _get_user_token(self):
        data = {
            "login": "ordinary_user1",
            "password": "ordinary_user1"
        }
        response = self.client.post('/auth/login/', data=data)
        return response.data['token']

    def full_url(self, url):
        return self.base_url + url

    def _make_job(self, otl_query):
        now_timestamp = int(datetime.now().timestamp())
        yesterday_timestamp = int(datetime.now().timestamp()) - 60 * 60 * 24
        data = {
            'otl_query': otl_query,
            'tws': now_timestamp,
            'twf': yesterday_timestamp
        }

        response = self.client.post(
            self.full_url('/makejob/'),
            data=data,
            format='json'
        )
        return response

    def make_job_success(self, otl_query):
        """
        Send make job query and checks success status
        Returns response data
        """

        response = self._make_job(otl_query)

        if response.status_code != 200:
            print(response.data)

        # checking status code
        self.assertEqual(response.status_code, 200)

        self.assertEqual(response.data['status'], 'success')

        self.assertEqual(len(response.data['job_id']), 32)

        # hash string 128 character
        self.assertEqual(len(response.data['path']), 128)

        return response.data

    def make_job_error(self, otl_query):
        """
        Send make job query and checks error state
        Returns response
        """
        response = self._make_job(otl_query)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.data['status'], 'error')
        return response

    def check_job(self, job_id):
        pass