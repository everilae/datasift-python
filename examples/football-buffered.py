# encoding: utf-8

# This example consumes 1% of the Twitter stream and outputs a . for each
# interaction received, and an X for each delete notification.
#
# NB: Most of the error handling (exception catching) has been removed for
# the sake of simplicity. Nearly everything in this library may throw
# exceptions, and production code should catch them. See the documentation
# for full details.

from __future__ import print_function
import time
import datasift.user
from datasift import config

print('Creating user...')
user = datasift.user.User(config.username, config.api_key)

print('Creating definition...')
csdl = 'interaction.content contains "football"'
definition = user.create_definition(csdl)
print('  %s' % csdl)

print('Getting buffered interactions...')
print('---')
num = 10
from_id = False
while num > 0:
    interactions = definition.get_buffered(num, from_id)
    for interaction in interactions:
        if 'deleted' in interaction:
            continue
        print('Type: %s' % (interaction['interaction']['type']))
        print('Content: %s' % (interaction['interaction']['content'].encode('ascii', 'replace')))
        print('--')
        num -= 1

    if num > 0:
        print('Sleeping (got %d of 10)...' % (10 - num))
        time.sleep(10)
        print('--')

print('Fetched 10 interactions, we\'re done.')
print()
