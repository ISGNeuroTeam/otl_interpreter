from rest.test import APIClient
from create_test_users import create_test_users


class BaseApiTest:
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