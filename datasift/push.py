# -*- coding: utf-8 -*-
from __future__ import absolute_import
from .user import User
from .exc import InvalidDataError


#-----------------------------------------------------------------------------
# The PushDefinition class.
#-----------------------------------------------------------------------------
class PushDefinition:
    """
    A PushDefinition instance represents a push endpoint configuration.
    """
    OUTPUT_PARAMS_PREFIX = 'output_params.'

    def __init__(self, user):
        """
        Initialise a PushDefinition object.
        """
        self._initial_status = ''
        self._output_type = ''
        self._output_params = {}

        if not isinstance(user, User):
            raise InvalidDataError('Please supply a valid User object when creating a PushDefinition object.')
        self._user = user

    def get_initial_status(self):
        """
        Get the initial status for subscriptions.
        """
        return self._initial_status

    def set_initial_status(self, status):
        """
        Set the initial status for subscriptions.
        """
        self._initial_status = status

    def get_output_type(self):
        """
        Get the output type.
        """
        return self._output_type

    def set_output_type(self, output_type):
        """
        Set the output type.
        """
        self._output_type = output_type

    def get_output_param(self, key):
        """
        Get an output parameter.
        """
        return self._output_params[self.OUTPUT_PARAMS_PREFIX + key]

    def get_output_params(self):
        """
        Get all of the output parameters.
        """
        return self._output_params

    def set_output_param(self, key, val):
        """
        Set an output parameter.
        """
        self._output_params[self.OUTPUT_PARAMS_PREFIX + key] = val

    def validate(self):
        """
        Validate the output type and parameters with the DataSift API.
        """
        params = {'output_type': self.get_output_type()}
        for key in self._output_params:
            params[key] = self._output_params[key]

        retval = self._user.call_api('push/validate', params)

    def subscribe_definition(self, definition, name):
        """
        Subscribe this endpoint to a Definition.
        """
        return self.subscribe_stream_hash(definition.get_hash(), name)

    def subscribe_stream_hash(self, hash_, name):
        """
        Subscribe this endpoint to a stream hash.
        """
        return self.subscribe('hash', hash_, name)

    def subscribe_historic(self, historic, name):
        """
        Subscribe this endpoint to a Historic.
        """
        return self.subscribe_historic_playback_id(historic.get_hash(), name)

    def subscribe_historic_playback_id(self, playback_id, name):
        """
        Subscribe this endpoint to a historic playback ID.
        """
        return self.subscribe('playback_id', playback_id, name)

    def subscribe(self, hash_type, hash_, name):
        """
        Subscribe this endpoint to a stream hash or historic playback ID. Note
        that this will activate the subscription if the initial status is set
        to active.
        """
        params = {
            'name': name,
            hash_type: hash_,
            'output_type': self.get_output_type()}

        for key in self._output_params:
            params[key] = self._output_params[key]

        if len(self.get_initial_status()) > 0:
            params['initial_status'] = self.get_initial_status()

        return PushSubscription(self._user, self._user.call_api('push/create', params))


#-----------------------------------------------------------------------------
# The PushSubscription class.
#-----------------------------------------------------------------------------
class PushSubscription(PushDefinition):
    """
    A PushSubscription instance represents the subscription of a push endpoint
    either a stream hash or a historic playback ID.
    """
    HASH_TYPE_STREAM = 'stream'
    HASH_TYPE_HISTORIC = 'historic'

    STATUS_ACTIVE = 'active'
    STATUS_PAUSED = 'paused'
    STATUS_STOPPED = 'stopped'
    STATUS_FINISHING = 'finishing'
    STATUS_FINISHED = 'finished'
    STATUS_FAILED = 'finished'
    STATUS_DELETED = 'deleted'

    ORDERBY_ID = 'id'
    ORDERBY_CREATED_AT = 'created_at'
    ORDERBY_REQUEST_TIME = 'request_time'

    ORDERDIR_ASC = 'asc'
    ORDERDIR_DESC = 'desc'

    @staticmethod
    def get(user, id):
        """
        Get a push subscription by ID.
        """
        return PushSubscription(user, user.call_api('push/get', {'id': id}))

    @staticmethod
    def list(user, page=1, per_page=20, order_by=False, order_dir=False,
             include_finished=False, hash_type=False, hash_=False):
        """
        Get a page of push subscriptions in the given user's account, where
        each page contains up to per_page items. Results will be ordered
        according to the supplied ordering parameters.
        """
        if page < 1:
            raise InvalidDataError('The specified page number is invalid')

        if per_page < 1:
            raise InvalidDataError('The specified per_page value is invalid')

        if order_by is False:
            order_by = PushSubscription.ORDERBY_CREATED_AT

        if order_dir is False:
            order_dir = PushSubscription.ORDERDIR_ASC

        params = {
            'page': page,
            'per_page': per_page,
            'order_by': order_by,
            'order_dir': order_dir
        }

        if hash_type is not False and hash_ is not False:
            params[hash_type] = hash_

        if include_finished == 1:
            params['include_finished'] = 1

        res = user.call_api('push/get', params)

        retval = {
            'count': res['count'],
            'subscriptions': []}

        for subscription in res['subscriptions']:
            retval['subscriptions'].append(PushSubscription(user, subscription))

        return retval

    @classmethod
    def list_by_stream_hash(cls, user, hash_, page=1, per_page=20, order_by=False, order_dir=False):
        """
        Get a page of push subscriptions in the given user's account
        subscribed to the given stream hash, where each page contains up to
        per_page items. Results will be ordered according to the supplied
        ordering parameters.
        """
        return cls.list(user, page, per_page, order_by, order_dir, False, 'hash', hash_)

    @classmethod
    def list_by_playback_id(cls, user, playback_id, page=1, per_page=20, order_by=False, order_dir=False):
        """
        Get a page of push subscriptions in the given user's account
        subscribed to the given playback ID, where each page contains up to
        per_page items. Results will be ordered according to the supplied
        ordering parameters.
        """
        return cls.list(user, page, per_page, order_by, order_dir, False, 'playback_id', playback_id)

    @staticmethod
    def get_logs(user, page=1, per_page=20, order_by=False, order_dir=False, id=False):
        """
        Page through recent push subscription log entries, specifying the sort
        order.
        """
        if page < 1:
            raise InvalidDataError('The specified page number is invalid')

        if per_page < 1:
            raise InvalidDataError('The specified per_page value is invalid')

        if order_by is False:
            order_by = PushSubscription.ORDERBY_REQUEST_TIME

        if order_dir is False:
            order_dir = PushSubscription.ORDERDIR_DESC

        params = {
            'page': page,
            'per_page': per_page,
            'order_by': order_by,
            'order_dir': order_dir
        }

        if id is not False:
            params['id'] = id

        return user.call_api('push/log', params)

    def __init__(self, user, data):
        """
        Initialise a new object from data in a dict.
        """
        self._user = False
        self._id = ''
        self._created_at = ''
        self._name = ''
        self._status = ''
        self._hash = ''
        self._hash_type = ''
        self._last_request = None
        self._last_success = None
        self._output_type = None
        self._output_params = None

        PushDefinition.__init__(self, user)
        self._init(data)

    def _init(self, data):
        """
        Populate this object from the data in a dict.
        """
        if not 'id' in data:
            raise InvalidDataError('No id found in subscription data')
        self._id = data['id']

        if not 'name' in data:
            raise InvalidDataError('No name found in subscription data')
        self._name = data['name']

        if not 'created_at' in data:
            raise InvalidDataError('No created_at found in subscription data')
        self._created_at = data['created_at']

        if not 'status' in data:
            raise InvalidDataError('No status found in subscription data')
        self._status = data['status']

        if not 'hash_type' in data:
            raise InvalidDataError('No hash_type found in subscription data')
        self._hash_type = data['hash_type']

        if not 'hash' in data:
            raise InvalidDataError('No hash found in subscription data')
        self._hash = data['hash']

        if not 'last_request' in data:
            raise InvalidDataError('No last_request found in subscription data')
        self._last_request = data['last_request']

        if not 'last_success' in data:
            raise InvalidDataError('No last_success found in subscription data')
        self._last_success = data['last_success']

        if not 'output_type' in data:
            raise InvalidDataError('No output_type found in subscription data')
        self._output_type = data['output_type']

        if not 'output_params' in data:
            raise InvalidDataError('No output_params found in subscription data')
        self._output_params = self._parse_output_params(data['output_params'])

    def _parse_output_params(self, params, prefix=''):
        """
        Recursive method to parse the output_params as received from the API
        into the flattened, dot-notation used by the client libraries.
        """
        retval = {}
        for key in params:
            if isinstance(params[key], dict):
                res = self._parse_output_params(params[key], '%s%s.' % (prefix, key))
                for key in res:
                    retval[key] = res[key]

            else:
                retval['%s%s' % (prefix, key)] = params[key]

        return retval

    def reload(self):
        """
        Re-fetch this subscription from the API.
        """
        self._init(self._user.call_api('push/get', { 'id': self.get_id() }))

    def get_id(self):
        """
        Return the subscription ID.
        """
        return self._id

    def get_name(self):
        """
        Return the subscription name.
        """
        return self._name

    def set_output_param(self, key, val):
        """
        Set an output parameter. Checks to see if the subscription has been
        deleted, and if not calls the base class to set the parameter.
        """
        if self.is_deleted():
            raise InvalidDataError('Cannot modify a deleted subscription')
        PushDefinition.set_output_param(self, key, val)

    def get_created_at(self):
        """
        Get the timestamp when this subscription was created.
        """
        return self._created_at

    def get_status(self):
        """
        Get the current status of this subscription. Make sure you call reload
        to get the latest data for this subscription first.
        """
        return self._status

    def is_deleted(self):
        """
        Returns True if this subscription has been deleted.
        """
        return self.get_status() == self.STATUS_DELETED

    def get_hash_type(self):
        """
        Get the hash type to which this subscription is subscribed.
        """
        return self._hash_type

    def get_hash(self):
        """
        Get the hash or playback ID to which this subscription is subscribed.
        """
        return self._hash

    def get_last_request(self):
        """
        Get the timestamp of the last push request.
        """
        return self._last_request

    def get_last_success(self):
        """
        Get the timestamp of the last successful push request.
        """
        return self._last_success

    def save(self):
        """
        Save changes to the name and output parameters of this subscription.
        """
        params = {
            'id': self.get_id(),
            'name': self.get_name()}

        for key in self.get_output_params():
            params['%s%s' % (self.OUTPUT_PARAMS_PREFIX, key)] = self.get_output_param(key)

        self._init(self._user.call_api('push/update', params))

    def pause(self):
        """
        Pause this subscription.
        """
        self._init(self._user.call_api('push/pause', { 'id': self.get_id() }))

    def resume(self):
        """
        Resume this subscription.
        """
        self._init(self._user.call_api('push/resume', { 'id': self.get_id() }))

    def stop(self):
        """
        Stop this subscription.
        """
        self._init(self._user.call_api('push/stop', { 'id': self.get_id() }))

    def delete(self):
        """
        Delete this subscription.
        """
        self._user.call_api('push/delete', {'id': self.get_id()})
        # The delete API call doesn't return the object, so set the status
        # manually
        self._status = self.STATUS_DELETED

    def get_log(self, page=1, per_page=20, order_by=False, order_dir=False):
        """
        Get a page of the log for this subscription in the order specified.
        """
        return PushSubscription.get_logs(self._user, page, per_page, order_by, order_dir, self.get_id())
