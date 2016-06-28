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
    apiKey = 'sdk-7c55610f-385f-46c5-a3a6-2fdc9ccf3034'
    config = Config(stream=True)
    client = LDClient(apiKey, config)
    user = {u'key': 'userKey'}
    time.sleep(5)
    print(client.toggle("update-app", user, False))
    print(client.api_key)

    client._stop_consumers()
