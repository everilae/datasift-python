# -*- coding: utf-8 -*-
from __future__ import absolute_import
from datetime import datetime
from .exc import InvalidDataError, CompileFailedError, APIError


#-----------------------------------------------------------------------------
# The Definition class.
#-----------------------------------------------------------------------------
class Definition(object):
    """
    A Definition instance represents a stream definition.
    """
    def __init__(self, user, csdl='', hash_=None):
        """
        Initialise a Definition object, optionally priming it with the given CSDL and/or
        hash.
        """
        self._created_at = None
        self._total_dpu = None
        self._csdl = None

        if not isinstance(user, User):
            raise InvalidDataError(
                'Please supply a valid User object when creating a '
                'Definition object.')

        self._user = user
        self._hash = hash_
        self.set(csdl)

    def get(self):
        """
        Get the definition's CSDL string.
        """
        if self._csdl is None:
            raise InvalidDataError('CSDL not available')

        return self._csdl

    def set(self, csdl):
        """
        Set the definition string.
        """
        if csdl is not None:
            if isinstance(csdl, str):
                csdl = csdl.encode('utf-8')

            elif not isinstance(csdl, bytes):
                raise InvalidDataError('Definitions must be UTF-8 byte strings.')

            csdl = csdl.strip()

            # Reset the hash if the CSDL hash changed
            if self._csdl is not None and self._csdl != csdl:
                self.clear_hash()

        self._csdl = csdl

    def get_hash(self):
        """
        Returns the hash for this definition. If the hash has not yet been
        obtained it compiles the definition first.
        """
        if self._hash is None:
            self.compile()

        return self._hash

    def clear_hash(self):
        """
        Reset the hash to false. The effect of this is to mark the definition
        as requiring compilation. Also resets other variables that depend on
        the CSDL.
        """
        if self._csdl is None:
            raise InvalidDataError(
                'Cannot clear the hash of a hash-only definition object')

        self._hash = None
        self._created_at = None
        self._total_dpu = None

    def get_created_at(self):
        """
        Returns the date when the stream was first created. If the created at
        date has not yet been obtained it validates the definition first.
        """
        if self._csdl is None:
            raise InvalidDataError('Created at date not available')

        if self._created_at is None:
            try:
                self.validate()

            except InvalidDataError:
                pass

        return self._created_at

    def get_total_dpu(self):
        """
        Returns the total DPU of the stream. If the DPU has not yet been
        obtained it validates the definition first.
        """
        if self._csdl is None:
            raise InvalidDataError('Total DPU not available')

        if self._total_dpu is None:
            try:
                self.validate()

            except InvalidDataError:
                pass

        return self._total_dpu

    def compile(self):
        """
        Call the DataSift API to compile this definition. If compilation
        succeeds we store the details in the response.
        """
        if not self._csdl:
            raise InvalidDataError('Cannot compile an empty definition')

        try:
            res = self._user.call_api('compile', {'csdl': self._csdl})

            if not 'hash' in res:
                raise CompileFailedError('Compiled successfully but no hash in the response')
            self._hash = res['hash']

            if not 'created_at' in res:
                raise CompileFailedError('Compiled successfully but no created_at in the response')
            self._created_at = datetime.strptime(res['created_at'], '%Y-%m-%d %H:%M:%S')

            if not 'dpu' in res:
                raise CompileFailedError('Compiled successfully but no DPU in the response')
            self._total_dpu = res['dpu']

        except APIError as e:
            msg, code = e.args
            self.clear_hash()

            if code == 400:
                raise CompileFailedError(msg)
            else:
                raise CompileFailedError('Unexpected APIError code: %d [%s]' % (code, msg))

    def validate(self):
        """
        Call the DataSift API to validate this definition. If validation
        succeeds we store the details in the response.
        """
        if not self._csdl:
            raise InvalidDataError('Cannot validate an empty definition')

        try:
            res = self._user.call_api('validate', {'csdl': self._csdl})

            if not 'created_at' in res:
                raise CompileFailedError('Validated successfully but no created_at in the response')

            self._created_at = datetime.strptime(res['created_at'], '%Y-%m-%d %H:%M:%S')

            if not 'dpu' in res:
                raise CompileFailedError('Validated successfully but no DPU in the response')

            self._total_dpu = res['dpu']

        except APIError as e:
            self.clear_hash()
            msg, code = e.args

            if code == 400:
                raise CompileFailedError(msg)

            else:
                raise CompileFailedError('Unexpected APIError code: %d [%s]' % (code, msg))

    def get_dpu_breakdown(self):
        """
        Call the DataSift API to get the DPU breakdown for this definition.
        """
        if not self._csdl:
            raise InvalidDataError('Cannot get the DPU breakdown for an empty definition')

        retval = self._user.call_api('dpu', {'hash': self.get_hash()})

        if not 'dpu' in retval:
            raise APIError('No total DPU value present in the breakdown data', -1)

        self._total_dpu = retval['dpu']
        return retval

    def get_buffered(self, count=None, from_id=None):
        """
        Call the DataSift API to get buffered interactions.
        """
        if not self._csdl:
            raise InvalidDataError('Cannot get buffered interactions for an empty definition')

        params = {'hash': self.get_hash()}
        if count is not None:
            params['count'] = count

        if from_id is not None:
            params['interaction_id'] = from_id

        retval = self._user.call_api('stream', params)

        if not 'stream' in retval:
            raise APIError('No data in the response', -1)

        return retval['stream']

    def create_historic(self, start, end, sources, sample, name):
        """
        Create a historic query based on this definition.
        """
        return Historic(self._user, self.get_hash(), start, end, sources, sample, name)

    def get_consumer(self, event_handler, consumer_type='http'):
        """
        Returns a StreamConsumer-derived object for this definition for the
        given type.
        """
        if not isinstance(event_handler, StreamConsumerEventHandler):
            raise InvalidDataError('Please supply an object derived from '
                                   'StreamConsumerEventHandler when '
                                   'requesting a consumer')
        return StreamConsumer.factory(self._user, consumer_type, self, event_handler)


from .user import User
from .historic import Historic
from .streamconsumer import StreamConsumer, StreamConsumerEventHandler
