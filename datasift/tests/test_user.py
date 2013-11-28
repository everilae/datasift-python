import unittest
import json
from datasift.tests import data
import datasift
from datasift import mockapiclient

class TestUser(unittest.TestCase):

    user = None
    mock_api_client = None

    def setUp(self):
        self.user = datasift.User(data.username, data.api_key)
        self.mock_api_client = datasift.mockapiclient.MockApiClient()
        self.user.set_api_client(self.mock_api_client)

    def test_construction(self):
        self.assertEqual(self.user.get_username(), data.username, 'Username is incorrect')
        self.assertEqual(self.user.get_api_key(), data.api_key, 'API key is incorrect')

    def test_create_definition_empty(self):
        definition = self.user.create_definition()
        self.assertEqual(definition.get(), b'', 'Definition is not empty')

    def test_create_definition_unicode(self):
        definition = self.user.create_definition(data.unicode_definition)
        self.assertEqual(definition.get(), data.definition, 'Definition is incorrect')

    def test_create_definition_nonempty(self):
        definition = self.user.create_definition(data.definition)
        self.assertEqual(definition.get(), data.definition, 'Definition is incorrect')

    def test_rate_limits(self):
        response = {
            'response_code': 200,
            'data': {
                'hash':       data.definition_hash,
                'created_at': '2011-12-13 14:15:16',
                'dpu':        10,
            },
            'rate_limit':           200,
            'rate_limit_remaining': 150,
        }
        self.mock_api_client.set_response(response)

        definition = self.user.create_definition(data.definition)

        try:
            definition.compile()
        except datasift.InvalidDataError as e:
            self.fail('InvalidDataError: %s' % (e))

        self.assertEqual(self.user.get_rate_limit(), response['rate_limit'], 'Rate limit is incorrect')
        self.assertEqual(self.user.get_rate_limit_remaining(), response['rate_limit_remaining'], 'Rate limit remaining is incorrect')

    def test_get_usage(self):
        response = {
            'response_code': 200,
            'data': json.loads('{"start":"Mon, 07 Nov 2011 10:25:00 +0000","end":"Mon, 07 Nov 2011 11:25:00 +0000","streams":{"6fd9d61afba0149e0f1d42080ccd9075":{"licenses":{"twitter":3},"seconds":300}}}'),
            'rate_limit':           200,
            'rate_limit_remaining': 150,
        }
        self.mock_api_client.set_response(response)

        usage = self.user.get_usage('day')
        self.assertEqual(usage, response['data'], 'Usage data for the specified day is not as expected')

    def test_get_usage_api_errors(self):
        try:
            response = {
                'response_code': 400,
                'data': {
                    'error': 'Bad request from user supplied data',
                },
                'rate_limit':           200,
                'rate_limit_remaining': 150,
            }
            self.mock_api_client.set_response(response)
            usage = self.user.get_usage()
            self.fail('Expected APIError was not thrown')
        except datasift.APIError as xxx_todo_changeme:
            (e, c) = xxx_todo_changeme.args
            self.assertEqual(response['data']['error'], e.__str__(), '400 exception message is not as expected')

        try:
            response = {
                'response_code': 401,
                'data': {
                    'error': 'User banned because they are a very bad person',
                },
                'rate_limit':           200,
                'rate_limit_remaining': 150,
            }
            self.mock_api_client.set_response(response)
            usage = self.user.get_usage()
            self.fail('Expected AccessDeniedError was not thrown')
        except datasift.AccessDeniedError as e:
            self.assertEqual(response['data']['error'], e.__str__(), '401 exception message is not as expected')

        try:
            response = {
                'response_code': 404,
                'data': {
                    'error': 'Endpoint or data not found',
                },
                'rate_limit':           200,
                'rate_limit_remaining': 150,
            }
            self.mock_api_client.set_response(response)
            usage = self.user.get_usage()
            self.fail('Expected APIError was not thrown')
        except datasift.APIError as xxx_todo_changeme1:
            (e, c) = xxx_todo_changeme1.args
            self.assertEqual(response['data']['error'], e.__str__(), '404 exception message is not as expected')

        try:
            response = {
                'response_code': 500,
                'data': {
                    'error': 'Problem with an internal service',
                },
                'rate_limit':           200,
                'rate_limit_remaining': 150,
            }
            self.mock_api_client.set_response(response)
            usage = self.user.get_usage()
            self.fail('Expected APIError was not thrown')
        except datasift.APIError as xxx_todo_changeme2:
            (e, c) = xxx_todo_changeme2.args
            self.assertEqual(response['data']['error'], e.__str__(), '500 exception message is not as expected')

if __name__ == '__main__':
    unittest.main()
