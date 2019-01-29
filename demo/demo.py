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
    ldclient.start_wait = 10
    ldclient.set_sdk_key('YOUR_SDK_KEY')

    user = {u'key': 'userKey'}
    print(ldclient.get().variation("update-app", user, False))

    ldclient.get().close()
