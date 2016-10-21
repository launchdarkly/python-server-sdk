from __future__ import print_function

import logging
import sys

import ldclient

root = logging.getLogger()
root.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)

if __name__ == '__main__':
    ldclient.sdk_key = 'sdk_key'
    ldclient.start_wait = 10
    client = ldclient.get()

    user = {u'key': 'userKey'}
    print(client.variation("update-app", user, False))

    client.close()
