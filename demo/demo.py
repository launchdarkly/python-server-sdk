from __future__ import print_function
from ldclient import LDClient

if __name__ == '__main__':
    apiKey = 'feefifofum'
    client = LDClient(apiKey)
    print(client.api_key)
