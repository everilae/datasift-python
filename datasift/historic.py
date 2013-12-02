# -*- coding: utf-8 -*-
from __future__ import absolute_import
from datetime import datetime
from .exc import (
    InvalidDataError,
    APIError,)


#-----------------------------------------------------------------------------
# The Historic class.
#-----------------------------------------------------------------------------
class Historic:
    """
    A Historic instance represents a historic query.
    """

    @staticmethod
    def list(user, page=1, per_page=20):
        """
        Get a page of Historics queries in the given user's account, where
        each page contains up to per_page items.
        """
        if page < 1:
            raise InvalidDataError('The specified page number is invalid')

        if per_page < 1:
            raise InvalidDataError('The specified per_page value is invalid')

        params = {
            'page': page,
            'max': per_page,
        }

        res = user.call_api('historics/get', params)

        retval = {
            'count': res['count'],
            'historics': []
        }
        for historic in res['data']:
            retval['historics'].append(Historic(user, historic))

        return retval

    def __init__(self, user, hash_, start=None, end=None, sources=None, sample=None, name=None):
        """
        Construct a new Historic query object from the supplied data. If the
        hash is a dict the object will be populated from that. If start is not
        supplied we'll attempt to load the object from the API using that as
        the playback ID. Otherwise a new Historics query is created using the
        data passed in.
        """
        self._playback_id = False
        self._dpus = False
        self._availability = {}
        self._hash = False
        self._start = False
        self._end = False
        self._created_at = False
        self._sample = 100
        self._sources = []
        self._name = ''
        self._status = 'created'
        self._progress = 0
        self._deleted = False

        if not isinstance(user, User):
            raise InvalidDataError('Please supply a valid User object when creating a Historic object.')

        self._user = user

        if isinstance(hash_, dict):
            # Initialising from a dict
            self._init(hash_)

        elif start is None:
            # The hash is the playback ID, get it from the API
            self._playback_id = hash_
            self.reload_data()

        else:
            # Creating a new Historics query
            if isinstance(hash_, Definition):
                hash_ = hash_.get_hash()

            if start == 0:
                raise InvalidDataError('Please supply a valid start timestamp')

            if end == 0:
                raise InvalidDataError('Please supply a valid end timestamp')

            if not isinstance(sources, list) or len(sources) == 0:
                raise InvalidDataError('Please supply a valid array of sources')

            self._hash = hash_
            self._start = start
            self._end = end
            self._sources = sources
            self._sample = sample
            self._name = name
            self._created_at = datetime.now()

    def _init(self, data):
        """
        Populate this object from the data in a dict.
        """
        if not 'id' in data:
            raise InvalidDataError('The playback ID is missing')
        self._playback_id = data['id']

        if not 'definition_id' in data:
            raise InvalidDataError('The stream hash is missing')
        self._hash = data['definition_id']

        if not 'name' in data:
            raise InvalidDataError('The name is missing')
        self._name = data['name']

        if not 'start' in data:
            raise InvalidDataError('The start timestamp is missing')
        self._start = data['start']

        if not 'end' in data:
            raise InvalidDataError('The end timestamp is missing')
        self._end = data['end']

        if not 'status' in data:
            raise InvalidDataError('The status is missing')
        self._status = data['status']

        if not 'progress' in data:
            raise InvalidDataError('The progress is missing')
        self._progress = data['progress']

        if not 'created_at' in data:
            raise InvalidDataError('The created at timestamp is missing')
        self._created_at = data['created_at']

        if not 'sources' in data:
            raise InvalidDataError('The sources is missing')
        self._sources = data['sources']

        if not 'sample' in data:
            raise InvalidDataError('The sample is missing')
        self._sample = data['sample']

        self._deleted = (self._status == 'deleted')

    def get_start_date(self):
        """
        Returns the start date for this query.
        """
        return self._start

    def get_end_date(self):
        """
        Returns the end date for this query.
        """
        return self._end

    def get_created_at(self):
        """
        Returns the created_at date for this query.
        """
        return self._created_at

    def get_name(self):
        """
        Returns the friendly name of this query.
        """
        return self._name

    def get_sources(self):
        """
        Returns the sources for this query.
        """
        return self._sources

    def get_progress(self):
        """
        Returns the progress percentage of this query.
        """
        return self._progress

    def get_sample(self):
        """
        Returns the sample percentage of this query.
        """
        return self._sample

    def get_status(self):
        """
        Returns the status of this query.
        """
        return self._status

    def set_name(self, name):
        """
        Set the friendly name for this query.
        """
        if self._deleted:
            raise InvalidDataError('Cannot set the name of a deleted Historics query')

        if self._playback_id is False:
            # Not prepared yet, just set it locally
            self._name = name

        else:
            # Already sent to the API, update the name via that API
            try:
                self._user.call_api(
                    'historics/update',
                    {'id': self._playback_id,
                     'name': self._name})
                self.reload_data()

            except APIError as e:
                m, c = e.args
                if c == 400:
                    # Missing or invalid parameters
                    raise InvalidDataError(m)

                else:
                    raise APIError('Unexpected APIError code: %d [%s]' % (c, e))

    def get_hash(self):
        """
        Get the playback ID for this query. If the query has not yet been
        prepared this will be done automagically to get the hash.
        """
        if self._playback_id is False:
            self.prepare()
        return self._playback_id

    def get_stream_hash(self):
        """
        Get the hash for the stream this Historics query is using.
        """
        return self._hash

    def get_dpus(self):
        """
        Get the DPU cost. If the query has not yet been prepared this will be
        done automagically to obtain the cost.
        """
        if self._dpus is False:
            self.prepare()
        return self._dpus

    def get_availability(self):
        """
        Get the data availability info. If the query has not yet been prepared
        this will be done automagically to obtain the availability data.
        """
        if self._availability is False:
            self.prepare()
        return self._availability

    def reload_data(self):
        if self._deleted:
            raise InvalidDataError('Cannot set the name of a deleted Historics query')

        if self._playback_id is False:
            raise InvalidDataError('Cannot reload the data for a Historics query that hasn\'t been prepared')

        try:
            self._init(self._user.call_api(
                'historics/get',
                {
                    'id': self._playback_id
                }))

        except APIError as e:
            m, c = e.args
            if c == 400:
                # Missing or invalid parameters
                raise InvalidDataError(m)
            else:
                raise APIError('Unexpected APIError code: %d [%s]' % (c, m))

    def prepare(self):
        """
        Call the DataSift API to prepare this historic query.
        """
        if self._deleted:
            raise InvalidDataError('Cannot prepare a deleted Historics query')

        if self._playback_id is not False:
            raise InvalidDataError('This historic query has already been prepared')

        try:
            res = self._user.call_api(
                'historics/prepare',
                {
                    'hash': self._hash,
                    'start': self._start,
                    'end': self._end,
                    'name': self._name,
                    'sources': ','.join(self._sources),
                    'sample': self._sample
                })

            if not 'id' in res:
                raise APIError('Prepared successfully but no playback ID in the response', -1)
            self._playback_id = res['id']

            if not 'dpus' in res:
                raise APIError('Prepared successfully but no DPU cost in the response', -1)
            self._dpus = res['dpus']

            if not 'availability' in res:
                raise APIError('Prepared successfully but no availability in the response', -1)
            self._availability = res['availability']
        except APIError as e:
            m, c = e.args
            if c == 400:
                # Missing or invalid parameters
                raise InvalidDataError(m)
            else:
                raise APIError('Unexpected APIError code: %d [%s]' % (c, m))

    def start(self):
        """
        Start this historic query.
        """
        if self._deleted:
            raise InvalidDataError('Cannot start a deleted Historics query')

        if self._playback_id is False or len(self._playback_id) == 0:
            raise InvalidDataError('Cannot start a historic query that hasn\'t been prepared')

        try:
            self._user.call_api(
                'historics/start',
                {'id': self._playback_id})

        except APIError as e:
            m, c = e.args
            if c == 400:
                # Missing or invalid parameters
                raise InvalidDataError(m)

            elif c == 404:
                # Historic query not found
                raise InvalidDataError(m)

            else:
                raise APIError('Unexpected APIError code: %d [%s]' % (c, m))

    def stop(self):
        """
        Stop this historic query.
        """
        if self._deleted:
            raise InvalidDataError('Cannot stop a deleted Historics query')

        if self._playback_id is False or len(self._playback_id) == 0:
            raise InvalidDataError('Cannot stop a historic query that hasn\'t been prepared')

        try:
            self._user.call_api(
                'historics/stop',
                {'id': self._playback_id})

        except APIError as e:
            e, c = e.args

            if c == 400:
                # Missing or invalid parameters
                raise InvalidDataError(e)

            elif c == 404:
                # Historic query not found
                raise InvalidDataError(e)

            else:
                raise APIError('Unexpected APIError code: %d [%s]' % (c, e))

    def delete(self):
        """
        Delete this historic query.
        """
        if self._deleted:
            raise InvalidDataError('This Historics query has already been deleted')

        if self._playback_id is False or len(self._playback_id) == 0:
            raise InvalidDataError('Cannot delete a historic query that hasn\'t been prepared')

        try:
            self._user.call_api(
                'historics/delete',
                {'id': self._playback_id})
            self._deleted = True

        except APIError as e:
            e, c = e.args

            if c == 400:
                # Missing or invalid parameters
                raise InvalidDataError(e)

            elif c == 404:
                # Historic query not found
                raise InvalidDataError(e)

            else:
                raise APIError('Unexpected APIError code: %d [%s]' % (c, e))


from .user import User
from .definition import Definition
