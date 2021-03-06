import datasift
import unittest
import datasift.user
import datasift.definition
import datasift.streamconsumer
import datasift.mockapiclient
from datasift.streamconsumer_http import (
    StreamConsumer_HTTP,
    StreamConsumer_HTTP_Thread)

try:
    import urllib.request
    urlopen_name = 'urllib.request.urlopen'
    request_name = 'urllib.request.Request'

except ImportError:
    import urllib2
    urlopen_name = 'urllib2.urlopen'
    request_name = 'urllib2.Request'

try:
    from unittest import mock

except ImportError:
    import mock


# CURRENTLY BROKEN SINCE WE SWITCHED TO USING RAW SOCKETS
class TestHttpStreamErrors(unittest.TestCase):
    """ Tests to ensure that the HTTP streamer implementation does not
    swallow errors raised by user-supplied event handler classes."""

    READ_CHUNK = "datasift.tests.test_http_stream.StreamConsumer_HTTP_Thread._read_chunk"

    @staticmethod
    def _make_stream(broken_method=None, is_running=True,
                     auto_reconnect=True):
        from datasift.tests import data
        user = datasift.user.User('fake', 'user')
        client = datasift.mockapiclient.MockApiClient()
        response = {
            'response_code': 200,
            'data': {
                'hash': data.definition_hash,
                'created_at': '2011-12-13 14:15:16',
                'dpu': 10,
            },
            'rate_limit': 200,
            'rate_limit_remaining': 150,
        }
        client.set_response(response)
        user.set_api_client(client)
        definition = datasift.definition.Definition(user, 'some cdsl')
        handler = BrokenHandler(broken_method)
        consumer = StreamConsumer_HTTP(user, definition, handler)
        if is_running:
            consumer._state = consumer.STATE_RUNNING
        return StreamConsumer_HTTP_Thread(consumer,
                                          auto_reconnect=auto_reconnect)

    def _check(self, sc):
        # Prefer self.assertRaises in future Python versions
        try:
            sc.run()

        except UserException:
            pass

        else:
            self.fail('UserException not raised')

    def _setup_mocks(self, request, urlopen):
        request.return_value = mock.Mock(name='request')
        response = mock.Mock(name='response')
        urlopen.return_value = response
        response.getcode.return_value = 200
        response.info.return_value = {}
        return response

    @mock.patch(READ_CHUNK)
    @mock.patch(urlopen_name)
    @mock.patch(request_name)
    def test_connect_exception(self, request, urlopen, mock_read_chunk):
        self._setup_mocks(request, urlopen)
        sc = self._make_stream('on_connect', True)
        self._check(sc)

    @mock.patch(READ_CHUNK)
    @mock.patch(urlopen_name)
    @mock.patch(request_name)
    def test_interaction_exception(self, request, urlopen, mock_read_chunk):
        self._setup_mocks(request, urlopen)
        sc = self._make_stream('on_interaction', True)
        mock_read_chunk.return_value = '{"interaction": "json"}'
        self._check(sc)

    @mock.patch(READ_CHUNK)
    @mock.patch(urlopen_name)
    @mock.patch(request_name)
    def test_deleted_exception(self, request, urlopen, mock_read_chunk):
        self._setup_mocks(request, urlopen)
        sc = self._make_stream('on_deleted', True)
        mock_read_chunk.return_value = '{"interaction": "x", "deleted": "1"}'
        self._check(sc)

    @mock.patch(READ_CHUNK)
    @mock.patch(urlopen_name)
    @mock.patch(request_name)
    def test_warning_exception(self, request, urlopen, mock_read_chunk):
        response = self._setup_mocks(request, urlopen)
        sc = self._make_stream('on_warning', True)
        response.readline.return_value = (
            '{"status": "warning", "message":'' "foo"}'
        )
        mock_read_chunk.return_value = '{"status": "warning", "message":'' "foo"}'
        self._check(sc)

    @mock.patch(READ_CHUNK)
    @mock.patch(urlopen_name)
    @mock.patch(request_name)
    def test_error_exception(self, request, urlopen, mock_read_chunk):
        response = self._setup_mocks(request, urlopen)
        sc = self._make_stream('on_error', True)
        response.readline.return_value = (
            '{"status": "error", "message":'' "foo"}'
        )
        self._check(sc)

    @mock.patch(READ_CHUNK)
    @mock.patch(urlopen_name)
    @mock.patch(request_name)
    def test_disconnect_exception(self, request, urlopen, mock_read_chunk):
        self._setup_mocks(request, urlopen)
        sc = self._make_stream('on_disconnect', False)
        self._check(sc)


class UserException(Exception):
    """ Custom exception that we can explicitly test for """
    pass


class BrokenHandler(datasift.streamconsumer.StreamConsumerEventHandler):

    def __init__(self, broken_method=None):
        self.broken_method = broken_method

    def on_connect(self, consumer):
        if self.broken_method == 'on_connect':
            raise UserException()

    def on_interaction(self, consumer, interaction, hash_):
        if self.broken_method == 'on_interaction':
            raise UserException()

    def on_deleted(self, consumer, interaction, hash_):
        if self.broken_method == 'on_deleted':
            raise UserException()

    def on_warning(self, consumer, msg):
        if self.broken_method == 'on_warning':
            raise UserException()

    def on_error(self, consumer, msg):
        if self.broken_method == 'on_error':
            raise UserException()

    def on_disconnect(self, consumer):
        if self.broken_method == 'on_disconnect':
            raise UserException()


class UserCreateStreamTestCase(unittest.TestCase):
    """Test for a bug where creating a stream from a user failed if
    attempting to create a stream for multiple hashes
    """

    def test_user_create_stream(self):
        user = datasift.user.User("ignored", "ignored")
        consumer = user.get_multi_consumer(["somehash", "someotherhash"],
                                           "ignored",
                                           "http")
        # Errors out because of missing underscore in _get_url()
        expected_url = "https://stream.datasift.com/multi?hashes=somehash,someotherhash"
        self.assertEqual(consumer._get_url(), expected_url)


if __name__ == '__main__':
    unittest.main()
