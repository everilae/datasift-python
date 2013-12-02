# -*- coding: utf-8 -*-
from __future__ import absolute_import
import json
import sys
from .user import User
from .definition import Definition
from .exc import InvalidDataError


#-----------------------------------------------------------------------------
# The StreamConsumerEventHandler base class.
#-----------------------------------------------------------------------------
class StreamConsumerEventHandler(object):
    """
    A base class for implementing event handlers for StreamConsumers.
    """
    def on_connect(self, consumer):
        pass

    def on_header(self, consumer, header):
        pass

    def on_interaction(self, consumer, interaction, hash_):
        pass

    def on_deleted(self, consumer, interaction, hash_):
        pass

    def on_warning(self, consumer, msg):
        pass

    def on_error(self, consumer, msg):
        pass

    def on_status(self, consumer, status, data):
        pass

    def on_disconnect(self, consumer):
        pass


#-----------------------------------------------------------------------------
# The StreamConsumer class. This class should never be used directly, but all
# protocol-specific StreamConsumers should inherit from it.
#-----------------------------------------------------------------------------
class StreamConsumer(object):
    """
    This is the base class for all protocol-specific StreamConsumer classes.
    """

    @staticmethod
    def factory(user, consumer_type, definition, event_handler):
        """
        Factory method for creating protocol-specific StreamConsumer objects.
        """
        module_name = 'datasift.streamconsumer_%s' % consumer_type

        try:
            __import__(module_name)

        except ImportError:
            raise InvalidDataError('Consumer type "%s" is unknown' % consumer_type)

        return sys.modules[module_name].factory(
            user, definition, event_handler)

    # Consumer type definitions.
    TYPE_HTTP = 'http'

    # Possible states.
    STATE_STOPPED = 0
    STATE_STARTING = 1
    STATE_RUNNING = 2
    STATE_STOPPING = 3

    def __init__(self, user, definition, event_handler):
        """
        Initialise a StreamConsumer object.
        """
        self._state = self.STATE_STOPPED
        self._auto_reconnect = True

        if not isinstance(user, User):
            raise InvalidDataError('Please supply a valid User object when creating a StreamConsumer object')

        self._user = user

        if isinstance(definition, str):
            self._hashes = self._user.create_definition(definition).get_hash()

        elif isinstance(definition, Definition):
            self._hashes = definition.get_hash()

        elif isinstance(definition, list):
            self._hashes = definition

        else:
            raise InvalidDataError('The definition must be a CSDL string, an array of hashes or a Definition object.')

        if len(self._hashes) == 0:
            raise InvalidDataError('No valid hashes found when creating the consumer.');

        self._event_handler = event_handler

    def consume(self, auto_reconnect=True):
        """
        Start consuming.
        """
        self._auto_reconnect = auto_reconnect
        self._state = StreamConsumer.STATE_STARTING
        self.on_start()

    def stop(self):
        """
        Stop the consumer.
        """
        if not self._is_running(True):
            raise InvalidDataError('Consumer state must be RUNNING before it can be stopped')

        self._state = StreamConsumer.STATE_STOPPING

    def _get_url(self):
        """
        Gets the URL for the required stream.
        """
        protocol = 'http'
        if self._user.use_ssl():
            protocol = 'https'

        if isinstance(self._hashes, list):
            return "%s://%smulti?hashes=%s" % (protocol, self._user._stream_base_url, ','.join(self._hashes))

        else:
            return "%s://%s%s" % (protocol, self._user._stream_base_url, self._hashes)

    def _get_auth_header(self):
        """
        Get the authorisation HTTP header.
        """
        return '%s:%s' % (self._user.get_username(), self._user.get_api_key())

    def _get_user_agent(self):
        """
        Get the user agent to send with the request.
        """
        return self._user.get_useragent()

    def _is_running(self, allow_starting=False):
        """
        Is the consumer running?
        """
        return ((allow_starting and
                 self._state is StreamConsumer.STATE_STARTING) or
                self._state is StreamConsumer.STATE_RUNNING)

    def _get_state(self):
        """
        Get the consumer state.
        """
        return self._state

    def _on_connect(self):
        """
        Called when the stream socket has connected.
        """
        # Allow state transition from STARTING only. Turned out that
        # if a caller is quick in calling 'consume' and 'stop', the
        # connection will lag a bit and cause state transitions:
        # STATE_STARTING -> STATE_STOPPING -> STATE_RUNNING
        # and the thread will run for ever (unless stop is called
        # again)
        if self._state is StreamConsumer.STATE_STARTING:
            self._state = self.STATE_RUNNING

        self._event_handler.on_connect(self)

    def _on_header(self, header):
        """
        Called when the stream socket has connected, header is a dictionary
        with the http headers of the stream's reponse.
        """
        self._event_handler.on_header(self, header)

    def _on_data(self, json_data):
        """
        Called for each complete chunk of JSON data is received.
        """
        try:
            data = json.loads(json_data)

        except Exception:
            if self._is_running():
                self._on_error('Failed to decode JSON: %s' % json_data)

        else:
            if 'status' in data:
                # Status notification
                if data['status'] == 'failure' or data['status'] == 'error':
                    self._on_error(data['message'])
                    self.stop()

                elif data['status'] == 'warning':
                    self._on_warning(data['message'])

                else:
                    status = data['status']
                    del data['status']
                    self._on_status(status, data)

            elif 'hash' in data:
                # Muli-stream data
                if 'deleted' in data['data'] and data['data']['deleted']:
                    self._event_handler.on_deleted(self, data['data'], data['hash'])

                else:
                    self._event_handler.on_interaction(self, data['data'], data['hash'])

            elif 'interaction' in data:
                # Single stream data
                if 'deleted' in data and data['deleted']:
                    self._event_handler.on_deleted(self, data, self._hashes)

                else:
                    self._event_handler.on_interaction(self, data, self._hashes)

            else:
                # Unknown message
                self._on_error('Unhandled data received: %s' % json_data)

    def _on_error(self, message):
        """
        Called when an error occurs. Errors are considered unrecoverable so
        we stop the consumer.
        """
        # Stop the consumer if we get an error
        if self._is_running():
            self.stop()
        self._event_handler.on_error(self, message)

    def _on_warning(self, message):
        """
        Called when a warning is raised or received.
        """
        self._event_handler.on_warning(self, message)

    def _on_status(self, status, data):
        """
        Called when a status message is raised or received.
        """
        self._event_handler.on_status(self, status, data)

    def _on_disconnect(self):
        """
        Called when the stream socket is disconnected.
        """
        self._event_handler.on_disconnect(self)
