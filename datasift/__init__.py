# -*- coding: utf-8 -*-

"""
The official DataSift API library for Python. This module provides access to
the REST API and also facilitates consuming streams.

Requires Python 2.4+.

To use, 'import datasift' and create a datasift.User object passing in your
username and API key. See the examples folder for reference usage.

Source Code:

https://github.com/datasift/datasift-python

Examples:

https://github.com/datasift/datasift-python/tree/master/examples

DataSift Platform Documentation:

http://dev.datasift.com/docs/

Copyright (C) 2012 MediaSift Ltd. All Rights Reserved.

This software is Open Source. Read the license:

https://github.com/datasift/datasift-python/blob/master/LICENSE
"""

from __future__ import absolute_import

try:
    import builtins

except ImportError:
    builtins = __builtins__

# Testing uses mock.patch on urllib.request/urllib2 members
try:
    import urllib.request
    urllib_request = urllib.request

except ImportError:
    import urllib2
    urllib_request = urllib2

try:
    from urllib.error import HTTPError, URLError

except ImportError:
    from urllib2 import HTTPError, URLError

try:
    from urllib.parse import urlencode

except ImportError:
    from urllib import urlencode

__author__ = "Stuart Dallas <stuart@3ft9.com>"
__status__ = "beta"
__version__ = "0.5.7"
__date__ = "28 June 2013"

#-----------------------------------------------------------------------------
# Module constants
#-----------------------------------------------------------------------------
USER_AGENT = 'DataSiftPython/%s' % __version__
