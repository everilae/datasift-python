# -*- coding: utf-8 -*-
from __future__ import absolute_import
from . import USER_AGENT
from .exc import APIError, RateLimitExceededError, AccessDeniedError
#-----------------------------------------------------------------------------
# Check for SSL support.
#-----------------------------------------------------------------------------
try:
    import ssl

except ImportError:
    SSL_AVAILABLE = False

else:
    SSL_AVAILABLE = True


#-----------------------------------------------------------------------------
# The User class - all interactions with the API should start here.
#-----------------------------------------------------------------------------
class User(object):
    """
    A User instance represents a DataSift user and provides access to all of
    the API functionality.
    """

    def __init__(self, username, api_key, use_ssl=True, stream_base_url='stream.datasift.com/'):
        """
        Initialise a User object with the given username and API key.
        """
        self._username = username
        self._api_key = api_key
        self._use_ssl = use_ssl

        if not stream_base_url.endswith('/'):
            stream_base_url += '/'

        self._stream_base_url = stream_base_url
        self._rate_limit = -1
        self._rate_limit_remaining = -1
        self._api_client = None

    def get_username(self):
        """
        Get the username.
        """
        return self._username

    def get_api_key(self):
        """
        Get the API key.
        """
        return self._api_key

    def use_ssl(self):
        """
        Returns true if stream connections should be using SSL.
        """
        return SSL_AVAILABLE and self._use_ssl

    def enable_ssl(self, use_ssl):
        """
        Set whether stream connections should use SSL.
        """
        self._use_ssl = use_ssl

    def get_rate_limit(self):
        """
        Get the rate limit returned by the last API call, or -1 if no API calls
        have been made since this object was created.
        """
        return self._rate_limit

    def get_rate_limit_remaining(self):
        """
        Get the rate limit remaining as returned by the last API call, or -1 if
        no API calls have been made since this object was created.
        """
        return self._rate_limit_remaining

    def set_api_client(self, api_client):
        """
        Set the object to be used as the API client. This must be a subclass
        of the default API client class.
        """
        self._api_client = api_client

    def get_usage(self, period='hour'):
        """
        Get usage data for this user.
        """
        return self.call_api('usage', {'period': period})

    def create_definition(self, csdl=''):
        """
        Create a definition object for this user. If a CSDL parameter is
        provided then this will be used as the initial CSDL for the
        definition.
        """
        return Definition(self, csdl)

    def create_historic(self, hash_, start, end, sources, sample, name):
        """
        Create a historic query based on this definition.
        """
        return Historic(self, hash_, start, end, sources, sample, name)

    def get_historic(self, playback_id):
        """
        Get an existing Historics query from the API.
        """
        return Historic(self, playback_id)

    def list_historics(self, page=1, per_page=20):
        """
        Get the Historics queries in your account.
        """
        return Historic.list(self, page, per_page)

    def create_push_definition(self):
        """
        Create a new Push definition for this user.
        """
        return PushDefinition(self)

    def get_push_subscription(self, subscription_id):
        """
        Get a Push subscription from the API.
        """
        return PushSubscription.get(self, subscription_id)

    def get_push_subscription_log(self, subscription_id=False):
        """
        Get the logs for all Push subscriptions or the given subscription.
        """
        if subscription_id is False:
            return PushSubscription.get_logs(self)

        else:
            return self.get_push_subscription(subscription_id).get_log()

    def list_push_subscriptions(self, page=1, per_page=20, order_by=False,
                                order_dir=False, include_finished=False,
                                hash_type=False, hash_=False):
        """
        Get the Push subscriptions in your account.
        """
        return PushSubscription.list(self, page, per_page, order_by, order_dir,
                                     include_finished, hash_type, hash_)

    def get_consumer(self, hash_, event_handler, consumer_type='http'):
        """
        Get a StreamConsumer object for the given hash via the given consumer
        type.
        """
        return StreamConsumer.factory(self, consumer_type,
                                      Definition(self, False, hash_),
                                      event_handler)

    def get_multi_consumer(self, hashes, event_handler, consumer_type='http'):
        """
        Get a StreamConsumer object for the given set of hashes via the given
        consumer type.
        """
        return StreamConsumer.factory(self, consumer_type, hashes, event_handler)

    @staticmethod
    def get_useragent():
        """
        Get the useragent to be used for all API requests.
        """
        return USER_AGENT

    def call_api(self, endpoint, params):
        """
        Make a call to a DataSift API endpoint.
        """
        if self._api_client is None:
            self._api_client = ApiClient()

        res = self._api_client.call(self.get_username(), self.get_api_key(),
                                    endpoint, params, self.get_useragent())

        self._rate_limit = res['rate_limit']
        self._rate_limit_remaining = res['rate_limit_remaining']

        if 200 <= res['response_code'] <= 299:
            retval = res['data']

        elif res['response_code'] == 401:
            errmsg = 'Authentication failed'
            if 'data' in res and 'error' in res['data']:
                errmsg = res['data']['error']

            raise AccessDeniedError(errmsg)

        else:
            if res['response_code'] == 403:
                if self._rate_limit_remaining == 0:
                    raise RateLimitExceededError(res['data']['comment'])

            errmsg = 'Unknown error (%d)' % res['response_code']
            if 'data' in res and 'error' in res['data']:
                errmsg = res['data']['error']

            raise APIError(errmsg, res['response_code'])

        return retval


from .definition import Definition
from .historic import Historic
from .apiclient import ApiClient
from .streamconsumer import StreamConsumer
from .push import PushDefinition, PushSubscription
