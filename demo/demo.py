from __future__ import print_function

import logging
import sys

import ldclient

root = logging.getLogger()
root.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)

if __name__ == '__main__':

    a = {}
    a['key'] = 0
    if a.get('key'):
        print(a['key'])

    #
    # ldclient._api_key = 'sdk-7c55610f-385f-46c5-a3a6-2fdc9ccf3034'
    # ldclient.start_wait = 10
    # client = ldclient.get()
    #
    # user = {u'key': 'userKey'}
    # print(client.toggle("update-app", user, False))
    #
    # client.close()
