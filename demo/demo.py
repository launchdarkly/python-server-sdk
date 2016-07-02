from __future__ import print_function
from ldclient import LDClient, Config
import logging
import sys
import time

root = logging.getLogger()
root.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)

if __name__ == '__main__':
    apiKey = 'your api key'
    client = LDClient(apiKey)
    print(client.api_key)

    user = {u'key': 'userKey'}
    print(client.toggle("update-app", user, False))

    time.sleep(10)
    client.close()
