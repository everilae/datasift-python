# encoding: utf-8

# This example mimics the Twitter track functionality. Run the script with
# any number of words or phrases as arguments and the script will create
# the equivalent CSDL and consume it as a stream, displaying matching
# interactions as they come in.
#
# NB: Most of the error handling (exception catching) has been removed for
# the sake of simplicity. Nearly everything in this library may throw
# exceptions, and production code should catch them. See the documentation
# for full details.

from __future__ import print_function
import sys
from datasift import config
import datasift.streamconsumer
import datasift.user

if len(sys.argv) < 2:
    print('ERR: Please specify the words and/or phrases to track!')
    sys.exit()

class EventHandler(datasift.streamconsumer.StreamConsumerEventHandler):
    def on_connect(self, consumer):
        print('Connected')
        print('--')

    def on_interaction(self, consumer, interaction, hash):
        if 'content' in interaction['interaction']:
            print(interaction['interaction']['content'].encode('ascii', 'replace'))
        else:
            print('%s interaction with no content' % interaction['interaction']['type'])
        print('--')

    def on_deleted(self, consumer, interaction, hash):
        print('Delete request for interaction %s of type %s.' % (interaction['interaction']['id'], interaction['interaction']['type']))
        print('Please delete it from your archive.')
        print('--')

    def on_warning(self, consumer, message):
        print('WARN: %s' % (message))
        print('--')

    def on_error(self, consumer, message):
        print('ERR: %s' % (message))
        print('--')

    def on_disconnect(self, consumer):
        print('Disconnected')

print('Creating user...')
user = datasift.user.User(config.username, config.api_key)

print('Creating definition...')
csdl = 'interaction.type == "twitter" and (interaction.content contains "%s")' % '" or interaction.content contains "'.join(sys.argv[1:])
definition = user.create_definition(csdl)
print('  %s' % csdl)

print('Getting the consumer...')
consumer = definition.get_consumer(EventHandler(), 'http')

print('Consuming...')
print('--')
consumer.consume()

consumer.run_forever()
