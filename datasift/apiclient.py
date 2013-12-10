# -*- coding: utf-8 -*-
from __future__ import absolute_import
import json
from . import (
    urllib_request,
    urlencode,
    HTTPError,
    URLError,
    USER_AGENT,)
from .exc import (
    APIError,)

API_BASE_URL = 'api.datasift.com/'


#-----------------------------------------------------------------------------
# The ApiClient class.
#-----------------------------------------------------------------------------
class ApiClient(object):
    """
    The default class used for accessing the DataSift API.
    """

    @staticmethod
    def call(username, api_key, endpoint, params={}, user_agent=USER_AGENT):
        """
        Make a call to a DataSift API endpoint.
        """
        url = 'http://%s%s.json' % (API_BASE_URL, endpoint)
        headers = {
            'Auth': '%s:%s' % (username, api_key),
            'User-Agent': user_agent,
        }
        # http://dev.datasift.com/docs/rest-api/things-every-developer-should-know#Formatting%20Parameters
        # "Parameters need to be formatted in UTF-8."
        req = urllib_request.Request(url,
                                     urlencode(params).encode('utf-8'),
                                     headers)

        try:
            resp = urllib_request.urlopen(req, None, 10)

        except HTTPError as err:
            resp = err

        except URLError as err:
            raise APIError('Request failed: %s' % err, 503)

        # Handle a response with no data
        content = resp.read()
        if len(content) == 0:
            data = json.loads('{}')

        else:
            data = json.loads(content.decode('utf-8'))

            if not data:
                raise APIError('Failed to decode the response', resp.getcode())

        retval = {
            'response_code': resp.getcode(),
            'data': data,
            'rate_limit': resp.headers.get('x-ratelimit-limit'),
            'rate_limit_remaining': resp.headers.get('x-ratelimit-remaining')}

        return retval
