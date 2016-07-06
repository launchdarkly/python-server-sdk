from __future__ import print_function

import logging
import sys

from ldclient import LDClient

root = logging.getLogger()
root.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)

if __name__ == '__main__':
    api_key = 'api_key'
    client = LDClient(api_key, start_wait=10)
    print(client.api_key)

    user = {u'key': 'userKey'}
    print(client.toggle("update-app", user, False))

    client.close()
