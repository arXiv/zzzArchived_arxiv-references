import unittest
from unittest import mock
import os

class TestCredentialsService(unittest.TestCase):
    @mock.patch('requests.get')
    def test_credentials_are_not_set(self, mock_get):
        mock_response = mock.MagicMock()
        mock_response.ok = True
        type(mock_response).json = mock.MagicMock(return_value={
          "Code": "Success",
          "LastUpdated": "2012-04-26T16:39:16Z",
          "Type": "AWS-HMAC",
          "AccessKeyId": "ASIAIOSFODNN7EXAMPLE",
          "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
          "Token": "token",
          "Expiration": "2017-05-17T15:09:54Z"
        })
        mock_get.return_value = mock_response

        from references.services.credentials import credentials
        self.assertEqual(credentials.session.aws_access_key_id,
                         "ASIAIOSFODNN7EXAMPLE",
                         "CredentialsSession should retrieve and set the"
                         " current access key id.")
        self.assertEqual(credentials.session.aws_secret_access_key,
                         "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                         "CredentialsSession should retrieve and set the"
                         " current secret access key.")
        self.assertEqual(credentials.session.aws_access_key_id,
                         os.environ.get('AWS_ACCESS_KEY_ID'),
                         "CredentialsSession should set AWS_ACCESS_KEY_ID"
                         " environment variable.")
        self.assertEqual(credentials.session.aws_secret_access_key,
                         os.environ.get('AWS_SECRET_ACCESS_KEY'),
                         "CredentialsSession should set AWS_SECRET_ACCESS_KEY"
                         " environment variable.")
